# Plan 1 — Foundations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the EirVah edge repo skeleton, three shared Python libraries, the OPC UA bottling-line simulator, and a single-node k3d cluster running NATS + Mosquitto + RabbitMQ + Prometheus + Grafana, so the "Bottling Line State" dashboard renders live device data from the simulator.

**Architecture:** uv-managed Python 3.12 monorepo with workspace members under `libs/` (shared contracts, NATS bus wrapper, observability helpers) and `services/` (one pod per directory; only `opcua-simulator` in this plan). Brokers and observability deployed via Kustomize bases under `deploy/k3s/base/`, composed into a `local` overlay applied to a single-node k3d cluster. Simulator emits state via OPC UA and exposes the same state as Prometheus gauges so Grafana renders device truth independent of the EirVah pipeline (which doesn't exist yet — that's Plans 2 and 3).

**Tech Stack:** Python 3.12, uv, ruff, mypy, pytest, pydantic v2, pydantic-settings, structlog, prometheus-client, starlette, uvicorn, nats-py, asyncua, python-ulid, k3d, kubectl, Kustomize, NATS server, Eclipse Mosquitto, RabbitMQ, Prometheus, Grafana.

**Spec reference:** `docs/superpowers/specs/2026-05-16-eirvah-edge-vertical-slice-design.md` (sections referenced inline by §-number).

---

## File structure produced by this plan

```
eirvah-edge-code/
├── .gitignore
├── .python-version
├── .pre-commit-config.yaml
├── README.md
├── pyproject.toml                              # uv workspace root
├── uv.lock                                     # generated
├── ruff.toml
├── mypy.ini
├── pytest.ini
│
├── libs/
│   ├── eirvah-contracts/                       # pydantic models, UNS topic helpers, golden fixtures
│   │   ├── pyproject.toml
│   │   ├── src/eirvah_contracts/
│   │   │   ├── __init__.py
│   │   │   ├── ulid.py                         # ULID generation
│   │   │   ├── uns.py                          # UNSPath model + topic-string helpers
│   │   │   ├── envelope.py                     # NATS envelope, correlation_id helpers
│   │   │   ├── signals.py                      # RawSignalEnvelope, NormalizedSignalEnvelope
│   │   │   ├── telemetry.py                    # TelemetryPayload v1.0 (spec §4.2)
│   │   │   └── actuation.py                    # ActuationRequest + result events v1.0 (spec §4.3)
│   │   └── tests/
│   │       ├── test_ulid.py
│   │       ├── test_uns.py
│   │       ├── test_envelope.py
│   │       ├── test_signals.py
│   │       ├── test_telemetry.py
│   │       ├── test_actuation.py
│   │       └── golden/
│   │           ├── telemetry_v1_0_sample.json
│   │           ├── actuation_request_v1_0_sample.json
│   │           ├── actuation_approve_v1_0_sample.json
│   │           └── actuation_reject_v1_0_sample.json
│   │
│   ├── eirvah-bus/                             # NATS req/rep + queue-group consumer helpers
│   │   ├── pyproject.toml
│   │   ├── src/eirvah_bus/
│   │   │   ├── __init__.py
│   │   │   ├── client.py                       # connect/close, correlation headers
│   │   │   ├── request_reply.py                # request_reply() with timeout
│   │   │   └── consumer.py                     # queue-group subscribe helper
│   │   └── tests/
│   │       ├── test_client.py
│   │       ├── test_request_reply.py
│   │       └── test_consumer.py
│   │
│   └── eirvah-observability/                   # metrics, logging, /healthz /readyz /metrics ASGI
│       ├── pyproject.toml
│       ├── src/eirvah_observability/
│       │   ├── __init__.py
│       │   ├── metrics.py                      # prometheus factory functions
│       │   ├── logging.py                      # structlog config with correlation_id
│       │   └── health.py                       # ASGI app exposing /healthz /readyz /metrics
│       └── tests/
│           ├── test_metrics.py
│           ├── test_logging.py
│           └── test_health.py
│
├── services/
│   └── opcua-simulator/                        # The bottling line (spec §9)
│       ├── pyproject.toml
│       ├── Dockerfile
│       ├── src/opcua_simulator/
│       │   ├── __init__.py
│       │   ├── __main__.py                     # entry point
│       │   ├── config.py                       # pydantic-settings (env-var driven)
│       │   ├── address_space.py                # YAML → OPC UA node tree
│       │   ├── rng.py                          # seeded PRNG wrapper
│       │   ├── temperature.py                  # mean-reverting walk dynamics
│       │   ├── motor.py                        # state machine + RPM
│       │   ├── throughput.py                   # depends on motor
│       │   ├── setpoint.py                     # write callback handling
│       │   ├── hot_spike.py                    # stochastic + OPC UA method trigger
│       │   ├── quality.py                      # bad/uncertain quality emission
│       │   ├── metrics.py                      # Prometheus gauges/counters for device state
│       │   └── server.py                       # OPC UA server bind + tick loop + /metrics ASGI
│       └── tests/
│           ├── test_address_space.py
│           ├── test_rng.py
│           ├── test_temperature.py
│           ├── test_motor.py
│           ├── test_throughput.py
│           ├── test_setpoint.py
│           ├── test_hot_spike.py
│           ├── test_quality.py
│           └── test_metrics.py
│
├── config/
│   └── opcua-address-space.yaml                # bottling-line model (mounted as ConfigMap)
│
├── deploy/
│   ├── k3s/
│   │   ├── base/
│   │   │   ├── kustomization.yaml
│   │   │   ├── namespace.yaml
│   │   │   ├── nats/                           # Deployment + Service
│   │   │   ├── mosquitto/                      # Deployment + Service + ConfigMap + Secret
│   │   │   ├── rabbitmq/                       # Deployment + Service + ConfigMap
│   │   │   ├── prometheus/                     # Deployment + Service + ConfigMap + RBAC
│   │   │   ├── grafana/                        # Deployment + Service + datasource + dashboards CM
│   │   │   └── opcua-simulator/                # Deployment + Service + ConfigMap (address space)
│   │   └── overlays/
│   │       └── local/
│   │           └── kustomization.yaml          # references base, no patches needed yet
│   │
│   └── grafana/
│       └── dashboards/
│           └── bottling-line-state.json        # second dashboard (spec §6.7)
│
└── scripts/
    ├── dev_up.sh                               # k3d up + build + import + apply
    ├── dev_down.sh                             # tear down
    └── build_all.sh                            # build every service image
```

**Files NOT created in this plan** (left to later plans):
- `services/opcua-data-subscriber/` and the other 10 telemetry/actuation pods — Plans 2 and 3.
- `tests/e2e/` — Plans 2 and 3.
- `scripts/trace.sh` — Plan 2.
- `deploy/grafana/dashboards/eirvah-edge-pipeline.json` — Plan 2 (telemetry half) and Plan 3 (actuation half).
- `deploy/k3s/overlays/lab/` — Plan 4.

---

## Conventions used in every task

1. **TDD by default.** Tests first, run them red, implement, run them green, commit.
2. **Each commit is small and focused.** One concept per commit.
3. **Working directory is the repo root** (`/Users/billy/Documents/research/eirvah-edge-code`) unless a step says otherwise.
4. **Python ≥ 3.12 required.** Verified in Task 1.
5. **Commit messages** use Conventional Commits style (`feat(scope): …`, `chore(scope): …`, `test(scope): …`).
6. **No emojis** in code, commits, or docs unless the spec calls for one.
7. **Every dependency must be OSI-approved open source** (memory rule). License is called out the first time a new dependency appears.

---

## Tasks in this plan (overview)

| # | Subject | Scope |
|---|---|---|
| 1 | Workspace skeleton + tooling config | pyproject.toml, ruff, mypy, pytest, .gitignore, .python-version |
| 2 | README and CLAUDE.md | Project orientation pointing at the spec |
| 3 | `eirvah-contracts`: ULID helpers | `libs/eirvah-contracts/src/eirvah_contracts/ulid.py` |
| 4 | `eirvah-contracts`: UNS path model + topic helpers | `uns.py` |
| 5 | `eirvah-contracts`: NATS envelope | `envelope.py` |
| 6 | `eirvah-contracts`: signal envelopes | `signals.py` |
| 7 | `eirvah-contracts`: telemetry payload v1.0 | `telemetry.py` + golden fixture |
| 8 | `eirvah-contracts`: actuation request + result events v1.0 | `actuation.py` + golden fixtures |
| 9 | `eirvah-bus`: NATS client wrapper | `client.py` |
| 10 | `eirvah-bus`: request-reply wrapper | `request_reply.py` |
| 11 | `eirvah-bus`: queue-group consumer | `consumer.py` |
| 12 | `eirvah-observability`: Prometheus metric factories | `metrics.py` |
| 13 | `eirvah-observability`: structlog config | `logging.py` |
| 14 | `eirvah-observability`: /healthz /readyz /metrics ASGI | `health.py` |
| 15 | `opcua-simulator`: service scaffolding | `pyproject.toml`, `Dockerfile`, `__main__.py`, `config.py` |
| 16 | `opcua-simulator`: address-space loader | `address_space.py` + sample config YAML |
| 17 | `opcua-simulator`: seeded PRNG | `rng.py` |
| 18 | `opcua-simulator`: temperature dynamics | `temperature.py` |
| 19 | `opcua-simulator`: motor state machine + RPM | `motor.py` |
| 20 | `opcua-simulator`: throughput dynamics | `throughput.py` |
| 21 | `opcua-simulator`: setpoint write handling | `setpoint.py` |
| 22 | `opcua-simulator`: hot-spike trigger | `hot_spike.py` |
| 23 | `opcua-simulator`: quality code emission | `quality.py` |
| 24 | `opcua-simulator`: Prometheus state metrics | `metrics.py` |
| 25 | `opcua-simulator`: OPC UA server + tick loop | `server.py` |
| 26 | Kustomize base: namespace | `deploy/k3s/base/namespace.yaml` + root kustomization |
| 27 | Kustomize base: NATS | `deploy/k3s/base/nats/` |
| 28 | Kustomize base: Mosquitto | `deploy/k3s/base/mosquitto/` |
| 29 | Kustomize base: RabbitMQ | `deploy/k3s/base/rabbitmq/` |
| 30 | Kustomize base: Prometheus | `deploy/k3s/base/prometheus/` |
| 31 | Kustomize base: Grafana | `deploy/k3s/base/grafana/` |
| 32 | Bottling Line State dashboard JSON | `deploy/grafana/dashboards/bottling-line-state.json` |
| 33 | Kustomize base: opcua-simulator | `deploy/k3s/base/opcua-simulator/` |
| 34 | `local` overlay | `deploy/k3s/overlays/local/` |
| 35 | `dev_up.sh`, `dev_down.sh`, `build_all.sh` scripts | `scripts/` |
| 36 | Plan 1 acceptance smoke test | manual checklist + tagged commit |

Now the tasks themselves.

---

### Task 1: Workspace skeleton + tooling config

**Files:**
- Create: `.python-version`
- Create: `.gitignore`
- Create: `pyproject.toml`
- Create: `ruff.toml`
- Create: `mypy.ini`
- Create: `pytest.ini`
- Create: `.pre-commit-config.yaml`

- [ ] **Step 1: Verify Python 3.12 and install uv**

Run:
```bash
python3.12 --version
which uv || curl -LsSf https://astral.sh/uv/install.sh | sh
uv --version
```
Expected: Python 3.12.x reported; uv ≥ 0.4 reported.

- [ ] **Step 2: Write `.python-version`**

```
3.12
```

- [ ] **Step 3: Write `.gitignore`**

```gitignore
# Python
__pycache__/
*.py[cod]
*$py.class
*.egg-info/
.eggs/
.venv/
venv/
build/
dist/

# uv
.uv/

# Test/coverage
.pytest_cache/
.coverage
.coverage.*
htmlcov/
coverage.xml
.mypy_cache/
.ruff_cache/

# IDE
.idea/
.vscode/
*.swp

# OS
.DS_Store

# Project-local
*.local.env
.tilt-cache/
```

- [ ] **Step 4: Write `pyproject.toml` (workspace root)**

```toml
[project]
name = "eirvah-edge-code"
version = "0.0.0"
description = "EirVah Edge Integration Layer — vertical slice"
readme = "README.md"
requires-python = ">=3.12"
authors = [{ name = "William Francis Stack" }]
license = { text = "Apache-2.0" }

[tool.uv]
package = false

[tool.uv.workspace]
members = [
    "libs/eirvah-contracts",
    "libs/eirvah-bus",
    "libs/eirvah-observability",
    "services/opcua-simulator",
]

[tool.uv.sources]
eirvah-contracts     = { workspace = true }
eirvah-bus           = { workspace = true }
eirvah-observability = { workspace = true }
```

- [ ] **Step 5: Write `ruff.toml`**

```toml
target-version = "py312"
line-length = 100

[lint]
select = ["E", "F", "I", "B", "UP", "SIM", "RUF"]
ignore = ["E501"]  # let formatter handle line length

[format]
quote-style = "double"
indent-style = "space"
```

- [ ] **Step 6: Write `mypy.ini`**

```ini
[mypy]
python_version = 3.12
strict = True
warn_return_any = True
warn_unused_ignores = True
disallow_untyped_defs = True
disallow_incomplete_defs = True
no_implicit_optional = True
namespace_packages = True
explicit_package_bases = True

[mypy-asyncua.*]
ignore_missing_imports = True
```

- [ ] **Step 7: Write `pytest.ini`**

```ini
[pytest]
testpaths = libs services tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
asyncio_mode = auto
filterwarnings =
    error
    ignore::DeprecationWarning:asyncua.*
```

- [ ] **Step 8: Write `.pre-commit-config.yaml`**

```yaml
repos:
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.6.9
    hooks:
      - id: ruff
        args: [--fix]
      - id: ruff-format
  - repo: https://github.com/pre-commit/mirrors-mypy
    rev: v1.13.0
    hooks:
      - id: mypy
        additional_dependencies:
          - pydantic>=2.8
          - pydantic-settings>=2.5
          - structlog>=24.4
          - prometheus-client>=0.20
          - nats-py>=2.7
          - asyncua>=1.1
          - python-ulid>=3.0
          - starlette>=0.40
```

- [ ] **Step 9: Sync (creates `.venv` and `uv.lock`)**

Run:
```bash
uv sync
```
Expected: `.venv/` and `uv.lock` created; no errors. (Lockfile will be empty/minimal until libs declare deps in later tasks.)

- [ ] **Step 10: Smoke-test tooling**

Run:
```bash
uv run ruff check .
uv run mypy --version
uv run pytest --collect-only
```
Expected: ruff passes (nothing to lint yet); mypy version printed; pytest reports "no tests ran" cleanly.

- [ ] **Step 11: Commit**

```bash
git add .python-version .gitignore pyproject.toml ruff.toml mypy.ini pytest.ini .pre-commit-config.yaml uv.lock
git commit -m "chore(workspace): initialize uv workspace and tooling config"
```

---

### Task 2: README and CLAUDE.md

**Files:**
- Create: `README.md`
- Modify: `CLAUDE.md` (currently a stub left by the user)

- [ ] **Step 1: Write `README.md`**

````markdown
# EirVah Edge Code

Edge Integration Layer for the **EirVah** reference architecture — a scalable, cost-efficient, open reference architecture for Unified Namespace (UNS) in Industrial IoT.

This repo is the implementation half of William Francis Stack's PhD work at the University of Žilina (supervisor: Aleš Janota). Scope is **the edge only** — protocol adapters, contextualizers, and publishers that translate industrial signals into the UNS over MQTT/AMQP, plus the actuation path back to devices. The cloud-side layers (persistence, decision/analytics) live in sibling repos.

## What's here

- A vertical-slice implementation of the Edge Integration Layer running on k3s, validated against a simulated bottling-line OPC UA device.
- All open source. No proprietary dependencies.

## Status

- **Plan 1 — Foundations:** in progress. Stand up the repo, shared libs, simulator, brokers, and observability.
- **Plan 2 — Telemetry path:** queued.
- **Plan 3 — Actuation path:** queued.
- **Plan 4 — Polish and reproducibility:** queued.

## Key documents

- Spec: [`docs/superpowers/specs/2026-05-16-eirvah-edge-vertical-slice-design.md`](docs/superpowers/specs/2026-05-16-eirvah-edge-vertical-slice-design.md)
- PhD proposal (companion): `UNIZA_Project_Proposal__EirVah__...pdf`

## Prerequisites

- macOS or Linux
- Python 3.12
- [uv](https://github.com/astral-sh/uv)
- Docker
- [k3d](https://k3d.io/)
- kubectl, kustomize

## Getting started (after Plan 1 is complete)

```bash
uv sync                       # install workspace + dev deps
./scripts/dev_up.sh           # create k3d cluster, build, deploy
# Grafana URL printed at the end; open it and switch to "Bottling Line State" dashboard
./scripts/dev_down.sh         # tear it all down
```

## Licensing

This repo is Apache-2.0 licensed. Every runtime and build dependency is OSI-approved open source.
````

- [ ] **Step 2: Update `CLAUDE.md`**

Replace the existing stub content with:

```markdown
# Project orientation for Claude

This repo is the **Edge Integration Layer** implementation of the EirVah reference architecture for Unified Namespace in Industrial IoT.

**Read this first:** `docs/superpowers/specs/2026-05-16-eirvah-edge-vertical-slice-design.md` — the canonical design.

## Hard rules

- Every dependency must be OSI-approved open source. Reject anything under SSPL, BSL, Elastic License, RSALv2, or other source-available licenses.
- Python 3.12. Type-checked with mypy strict mode.
- All services follow the layout under `services/<name>/src/<snake_case_name>/`.
- All shared code lives in `libs/eirvah-*/`.
- Pipeline orchestration lives in the orchestrator pods, not the workers.
- Internal edge communication: NATS. Public UNS surface: MQTT + AMQP.
- Tests come first. No code without a failing test that justifies it.

## Useful pointers

- Plans: `docs/superpowers/plans/`
- Architectural decisions: `docs/adr/` (added in Plan 4)
- Spec: `docs/superpowers/specs/`
- Toolchain: uv, ruff, mypy, pytest, k3d, kustomize, Tilt (Plans 2+).

## When implementing

- Match the file layout described in the plan you are executing.
- Don't introduce new top-level dependencies without flagging the license.
- Don't add features outside the current plan's scope without surfacing it.
```

- [ ] **Step 3: Delete the leftover `claude.md` (lowercase) stub if present**

Run:
```bash
[ -f claude.md ] && git rm -f claude.md || echo "already gone"
```
Expected: file removed or "already gone".

- [ ] **Step 4: Commit**

```bash
git add README.md CLAUDE.md
git commit -m "docs(repo): add README and project orientation for contributors"
```

---

### Task 3: `eirvah-contracts` — package skeleton and ULID helpers

**Files:**
- Create: `libs/eirvah-contracts/pyproject.toml`
- Create: `libs/eirvah-contracts/src/eirvah_contracts/__init__.py`
- Create: `libs/eirvah-contracts/src/eirvah_contracts/ulid.py`
- Create: `libs/eirvah-contracts/tests/__init__.py`
- Create: `libs/eirvah-contracts/tests/test_ulid.py`

- [ ] **Step 1: Write `libs/eirvah-contracts/pyproject.toml`**

```toml
[project]
name = "eirvah-contracts"
version = "0.0.0"
description = "Wire-schema models, UNS topic helpers, NATS envelopes."
requires-python = ">=3.12"
license = { text = "Apache-2.0" }
dependencies = [
    "pydantic>=2.8",
    "python-ulid>=3.0",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/eirvah_contracts"]
```

Note: `python-ulid` is Apache-2.0. License-clean.

- [ ] **Step 2: Write `libs/eirvah-contracts/src/eirvah_contracts/__init__.py`**

```python
"""Wire-schema models and helpers shared across all EirVah edge services."""
```

- [ ] **Step 3: Write the failing test `libs/eirvah-contracts/tests/test_ulid.py`**

```python
import re

import pytest

from eirvah_contracts.ulid import generate_correlation_id, is_valid_correlation_id


def test_generate_correlation_id_returns_26_char_crockford_base32() -> None:
    cid = generate_correlation_id()
    assert isinstance(cid, str)
    assert len(cid) == 26
    assert re.fullmatch(r"[0-9A-HJKMNP-TV-Z]{26}", cid) is not None


def test_generated_ids_are_unique_within_a_burst() -> None:
    ids = {generate_correlation_id() for _ in range(1000)}
    assert len(ids) == 1000


def test_generated_ids_are_lexicographically_sortable_by_time() -> None:
    earlier = generate_correlation_id()
    later = generate_correlation_id()
    assert earlier <= later  # ULID is monotonic given fast successive calls within ms


def test_is_valid_correlation_id_accepts_a_generated_id() -> None:
    assert is_valid_correlation_id(generate_correlation_id()) is True


@pytest.mark.parametrize(
    "bad",
    [
        "",
        "too-short",
        "01HZXC8P9G7Q3M6V0K2T8R5W4",       # 25 chars
        "01HZXC8P9G7Q3M6V0K2T8R5W4AX",     # 27 chars
        "01HZXC8P9G7Q3M6V0K2T8R5W4!",      # invalid char
        "01hzxc8p9g7q3m6v0k2t8r5w4a",      # lowercase not allowed
    ],
)
def test_is_valid_correlation_id_rejects_malformed(bad: str) -> None:
    assert is_valid_correlation_id(bad) is False
```

Also create `libs/eirvah-contracts/tests/__init__.py`:

```python
```

(empty file — marks the tests directory as a package).

- [ ] **Step 4: Run the test (red)**

Run:
```bash
uv sync
uv run pytest libs/eirvah-contracts/tests/test_ulid.py -v
```
Expected: import error or "no module named eirvah_contracts.ulid".

- [ ] **Step 5: Write `libs/eirvah-contracts/src/eirvah_contracts/ulid.py`**

```python
"""ULID-based correlation IDs.

ULIDs are 26-char Crockford Base32, time-prefixed and lexicographically sortable.
We use them as the single end-to-end traceability mechanism — see spec §4.2.
"""

from __future__ import annotations

import re

from ulid import ULID

_ULID_RE = re.compile(r"^[0-9A-HJKMNP-TV-Z]{26}$")


def generate_correlation_id() -> str:
    """Return a new ULID in canonical 26-char uppercase Crockford Base32."""
    return str(ULID())


def is_valid_correlation_id(value: str) -> bool:
    """True iff ``value`` is a syntactically valid uppercase ULID."""
    return bool(_ULID_RE.fullmatch(value))
```

- [ ] **Step 6: Run the test (green)**

Run:
```bash
uv run pytest libs/eirvah-contracts/tests/test_ulid.py -v
```
Expected: all tests pass.

- [ ] **Step 7: Type-check**

Run:
```bash
uv run mypy libs/eirvah-contracts/src
```
Expected: no errors.

- [ ] **Step 8: Commit**

```bash
git add libs/eirvah-contracts
git commit -m "feat(contracts): ULID-based correlation IDs"
```

---

### Task 4: `eirvah-contracts` — UNS path model and topic helpers

**Files:**
- Create: `libs/eirvah-contracts/src/eirvah_contracts/uns.py`
- Create: `libs/eirvah-contracts/tests/test_uns.py`

- [ ] **Step 1: Write the failing test `libs/eirvah-contracts/tests/test_uns.py`**

```python
import pytest
from pydantic import ValidationError

from eirvah_contracts.uns import UNSPath, parse_uns_topic, build_uns_topic


def _sample() -> UNSPath:
    return UNSPath(
        enterprise="uniza",
        site="zilina",
        area="factory1",
        line="line_a",
        cell="bottler",
        equipment="temperature_sensor_01",
        measurement="temperature",
    )


def test_build_topic_joins_seven_segments_with_slash() -> None:
    assert (
        build_uns_topic(_sample())
        == "uniza/zilina/factory1/line_a/bottler/temperature_sensor_01/temperature"
    )


def test_parse_round_trips_with_build() -> None:
    topic = "uniza/zilina/factory1/line_a/bottler/motor_01/rpm"
    path = parse_uns_topic(topic)
    assert build_uns_topic(path) == topic
    assert path.equipment == "motor_01"
    assert path.measurement == "rpm"


@pytest.mark.parametrize(
    "bad_topic",
    [
        "too/few/segments",
        "uniza/zilina/factory1/line_a/bottler/equipment",                 # 6 segments
        "uniza/zilina/factory1/line_a/bottler/equipment/x/y",             # 8 segments
        "uniza/zilina/factory1/line_a/bottler/equipment/UPPER",           # uppercase
        "uniza/zilina/factory1/line_a/bottler/equip-ment/measurement",    # hyphen
        "uniza//factory1/line_a/bottler/equipment/measurement",           # empty segment
        "uniza/zilina/factory1/line_a/bottler/equipment/m e a s",         # space
    ],
)
def test_parse_rejects_invalid_topics(bad_topic: str) -> None:
    with pytest.raises(ValueError):
        parse_uns_topic(bad_topic)


@pytest.mark.parametrize(
    "field, bad_value",
    [
        ("enterprise", "Uniza"),
        ("site", "zi-lina"),
        ("area", ""),
        ("equipment", "x y"),
        ("measurement", "TEMP"),
    ],
)
def test_segment_validation_rejects_disallowed_chars(field: str, bad_value: str) -> None:
    kwargs = dict(
        enterprise="uniza",
        site="zilina",
        area="factory1",
        line="line_a",
        cell="bottler",
        equipment="motor_01",
        measurement="rpm",
    )
    kwargs[field] = bad_value
    with pytest.raises(ValidationError):
        UNSPath(**kwargs)
```

- [ ] **Step 2: Run the test (red)**

Run:
```bash
uv run pytest libs/eirvah-contracts/tests/test_uns.py -v
```
Expected: ImportError on `eirvah_contracts.uns`.

- [ ] **Step 3: Write `libs/eirvah-contracts/src/eirvah_contracts/uns.py`**

```python
"""UNS hierarchy model and MQTT topic helpers.

Strict 7-level ISA-95 hierarchy (spec §4.1):

    {enterprise}/{site}/{area}/{line}/{cell}/{equipment}/{measurement}

Segments are lowercase ASCII; allowed characters [a-z0-9_].
"""

from __future__ import annotations

import re
from typing import Annotated

from pydantic import BaseModel, ConfigDict, StringConstraints

# A single UNS segment: lowercase, alphanumerics + underscore, non-empty.
_SEGMENT_RE = r"^[a-z0-9_]+$"

UNSSegment = Annotated[
    str,
    StringConstraints(pattern=_SEGMENT_RE, min_length=1, max_length=128),
]


class UNSPath(BaseModel):
    """The 7-level ISA-95 hierarchy that uniquely names a UNS measurement."""

    model_config = ConfigDict(frozen=True, str_strip_whitespace=False)

    enterprise: UNSSegment
    site: UNSSegment
    area: UNSSegment
    line: UNSSegment
    cell: UNSSegment
    equipment: UNSSegment
    measurement: UNSSegment


_LEVELS = ("enterprise", "site", "area", "line", "cell", "equipment", "measurement")


def build_uns_topic(path: UNSPath) -> str:
    """Join the 7 segments of *path* into an MQTT topic string."""
    return "/".join(getattr(path, level) for level in _LEVELS)


def parse_uns_topic(topic: str) -> UNSPath:
    """Parse a 7-segment UNS topic into a :class:`UNSPath`.

    Raises ``ValueError`` if the topic has the wrong number of segments,
    or ``pydantic.ValidationError`` if any segment is malformed.
    """
    segments = topic.split("/")
    if len(segments) != len(_LEVELS):
        raise ValueError(
            f"UNS topic must have exactly {len(_LEVELS)} segments; got {len(segments)}: {topic!r}"
        )
    if any(not _is_segment(seg) for seg in segments):
        raise ValueError(f"UNS topic contains an invalid segment: {topic!r}")
    return UNSPath(**dict(zip(_LEVELS, segments, strict=True)))


def _is_segment(value: str) -> bool:
    return re.fullmatch(_SEGMENT_RE, value) is not None
```

- [ ] **Step 4: Run the test (green)**

Run:
```bash
uv run pytest libs/eirvah-contracts/tests/test_uns.py -v
```
Expected: all tests pass.

- [ ] **Step 5: Type-check**

Run:
```bash
uv run mypy libs/eirvah-contracts/src
```
Expected: no errors.

- [ ] **Step 6: Commit**

```bash
git add libs/eirvah-contracts/src/eirvah_contracts/uns.py libs/eirvah-contracts/tests/test_uns.py
git commit -m "feat(contracts): UNS path model and topic helpers (spec §4.1)"
```

---

### Task 5: `eirvah-contracts` — NATS envelope

**Files:**
- Create: `libs/eirvah-contracts/src/eirvah_contracts/envelope.py`
- Create: `libs/eirvah-contracts/tests/test_envelope.py`

The NATS envelope is the wrapper every internal-bus message lives inside (spec §4.4). It carries the correlation ID, an optional context dict, and an optional error structure.

- [ ] **Step 1: Write the failing test `libs/eirvah-contracts/tests/test_envelope.py`**

```python
import json

import pytest

from eirvah_contracts.envelope import NATSEnvelope, EnvelopeError
from eirvah_contracts.ulid import generate_correlation_id


def test_envelope_round_trips_through_json() -> None:
    env = NATSEnvelope(
        correlation_id=generate_correlation_id(),
        payload={"value": 42},
        context={"stage": "convert"},
    )
    raw = env.model_dump_json()
    parsed = NATSEnvelope.model_validate_json(raw)
    assert parsed == env


def test_envelope_defaults_status_to_ok() -> None:
    env = NATSEnvelope(correlation_id=generate_correlation_id(), payload={"x": 1})
    assert env.status == "ok"
    assert env.error is None


def test_envelope_error_status() -> None:
    env = NATSEnvelope(
        correlation_id=generate_correlation_id(),
        status="error",
        error=EnvelopeError(kind="ValidationError", message="bad input"),
    )
    raw = env.model_dump_json()
    parsed = NATSEnvelope.model_validate_json(raw)
    assert parsed.status == "error"
    assert parsed.error is not None
    assert parsed.error.kind == "ValidationError"


def test_envelope_rejects_invalid_correlation_id() -> None:
    with pytest.raises(ValueError):
        NATSEnvelope(correlation_id="not-a-ulid", payload={})


def test_envelope_rejects_invalid_status() -> None:
    with pytest.raises(ValueError):
        NATSEnvelope(
            correlation_id=generate_correlation_id(),
            payload={},
            status="weird",  # type: ignore[arg-type]
        )


def test_envelope_serialises_with_compact_json() -> None:
    env = NATSEnvelope(correlation_id=generate_correlation_id(), payload={"v": 1})
    raw = env.model_dump_json()
    # No trailing whitespace, parseable
    assert json.loads(raw)["status"] == "ok"
```

- [ ] **Step 2: Run the test (red)**

Run:
```bash
uv run pytest libs/eirvah-contracts/tests/test_envelope.py -v
```
Expected: ImportError on `eirvah_contracts.envelope`.

- [ ] **Step 3: Write `libs/eirvah-contracts/src/eirvah_contracts/envelope.py`**

```python
"""NATS envelope shared by every internal edge message (spec §4.4)."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, field_validator

from eirvah_contracts.ulid import is_valid_correlation_id

Status = Literal["ok", "error"]


class EnvelopeError(BaseModel):
    """Structured error payload accompanying ``status="error"`` envelopes."""

    model_config = ConfigDict(extra="forbid")

    kind: str
    message: str


class NATSEnvelope(BaseModel):
    """The wrapper around every internal NATS message.

    ``payload`` is intentionally loosely typed (``dict[str, Any]``) here so the
    envelope can carry any of the schema-versioned domain payloads defined
    elsewhere in this package. The domain layer is responsible for narrowing.
    """

    model_config = ConfigDict(extra="forbid")

    correlation_id: str
    status: Status = "ok"
    payload: dict[str, Any] | None = None
    context: dict[str, Any] | None = None
    error: EnvelopeError | None = None

    @field_validator("correlation_id")
    @classmethod
    def _validate_correlation_id(cls, value: str) -> str:
        if not is_valid_correlation_id(value):
            raise ValueError(f"invalid correlation_id: {value!r}")
        return value
```

- [ ] **Step 4: Run the test (green)**

Run:
```bash
uv run pytest libs/eirvah-contracts/tests/test_envelope.py -v
uv run mypy libs/eirvah-contracts/src
```
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add libs/eirvah-contracts/src/eirvah_contracts/envelope.py libs/eirvah-contracts/tests/test_envelope.py
git commit -m "feat(contracts): NATS envelope with structured error"
```

---

### Task 6: `eirvah-contracts` — raw and normalized signal envelopes

**Files:**
- Create: `libs/eirvah-contracts/src/eirvah_contracts/signals.py`
- Create: `libs/eirvah-contracts/tests/test_signals.py`

These are the *internal* payloads the OPC UA data subscriber and data converter exchange. Distinct from the public telemetry payload in §4.2 — that comes in Task 7.

- [ ] **Step 1: Write the failing test `libs/eirvah-contracts/tests/test_signals.py`**

```python
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from eirvah_contracts.signals import (
    NormalizedSignalEnvelope,
    Quality,
    RawSignalEnvelope,
    SignalValueType,
)


def _ts() -> datetime:
    return datetime(2026, 5, 16, 13, 45, 22, 123456, tzinfo=timezone.utc)


def test_raw_signal_round_trips_through_json() -> None:
    raw = RawSignalEnvelope(
        source_endpoint="opc.tcp://opcua-simulator:4840",
        node_id="ns=2;s=Bottler.Temperature01",
        value=23.4,
        value_type="double",
        quality="good",
        source_timestamp=_ts(),
        server_timestamp=_ts(),
        received_at=_ts(),
    )
    parsed = RawSignalEnvelope.model_validate_json(raw.model_dump_json())
    assert parsed == raw


def test_normalized_signal_round_trips_through_json() -> None:
    normed = NormalizedSignalEnvelope(
        node_id="ns=2;s=Bottler.Temperature01",
        value=23.4,
        value_type="double",
        unit="degC",
        quality="good",
        source_timestamp=_ts(),
        received_at=_ts(),
    )
    parsed = NormalizedSignalEnvelope.model_validate_json(normed.model_dump_json())
    assert parsed == normed


@pytest.mark.parametrize("vt", ["double", "int64", "bool", "string"])
def test_value_type_accepts_v1_supported(vt: SignalValueType) -> None:
    RawSignalEnvelope(
        source_endpoint="opc.tcp://x",
        node_id="n",
        value=0,
        value_type=vt,
        quality="good",
        source_timestamp=_ts(),
        server_timestamp=_ts(),
        received_at=_ts(),
    )


def test_value_type_rejects_unsupported() -> None:
    with pytest.raises(ValidationError):
        RawSignalEnvelope(
            source_endpoint="opc.tcp://x",
            node_id="n",
            value=[1, 2, 3],
            value_type="array",  # type: ignore[arg-type]
            quality="good",
            source_timestamp=_ts(),
            server_timestamp=_ts(),
            received_at=_ts(),
        )


@pytest.mark.parametrize("q", ["good", "uncertain", "bad"])
def test_quality_codes(q: Quality) -> None:
    RawSignalEnvelope(
        source_endpoint="opc.tcp://x",
        node_id="n",
        value=0,
        value_type="double",
        quality=q,
        source_timestamp=_ts(),
        server_timestamp=_ts(),
        received_at=_ts(),
    )
```

- [ ] **Step 2: Run the test (red)**

Run:
```bash
uv run pytest libs/eirvah-contracts/tests/test_signals.py -v
```
Expected: ImportError on `eirvah_contracts.signals`.

- [ ] **Step 3: Write `libs/eirvah-contracts/src/eirvah_contracts/signals.py`**

```python
"""Internal signal envelopes (NOT the public telemetry payload — see telemetry.py)."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict

SignalValueType = Literal["double", "int64", "bool", "string"]
Quality = Literal["good", "uncertain", "bad"]
SignalValue = float | int | bool | str


class RawSignalEnvelope(BaseModel):
    """Emitted by ``opcua-data-subscriber`` onto ``uns.ingress.raw``."""

    model_config = ConfigDict(extra="forbid")

    source_endpoint: str
    node_id: str
    value: SignalValue
    value_type: SignalValueType
    quality: Quality
    source_timestamp: datetime
    server_timestamp: datetime
    received_at: datetime


class NormalizedSignalEnvelope(BaseModel):
    """Emitted by ``data-converter`` (the value has been unit-converted/scaled)."""

    model_config = ConfigDict(extra="forbid")

    node_id: str
    value: SignalValue
    value_type: SignalValueType
    unit: str
    quality: Quality
    source_timestamp: datetime
    received_at: datetime
```

- [ ] **Step 4: Run the test (green)**

Run:
```bash
uv run pytest libs/eirvah-contracts/tests/test_signals.py -v
uv run mypy libs/eirvah-contracts/src
```
Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add libs/eirvah-contracts/src/eirvah_contracts/signals.py libs/eirvah-contracts/tests/test_signals.py
git commit -m "feat(contracts): raw and normalized internal signal envelopes"
```

---

### Task 7: `eirvah-contracts` — TelemetryPayload v1.0 + golden fixture

This is the *public* MQTT payload — the actual research-contribution surface (spec §4.2).

**Files:**
- Create: `libs/eirvah-contracts/src/eirvah_contracts/telemetry.py`
- Create: `libs/eirvah-contracts/tests/test_telemetry.py`
- Create: `libs/eirvah-contracts/tests/golden/telemetry_v1_0_sample.json`

- [ ] **Step 1: Write the golden fixture `libs/eirvah-contracts/tests/golden/telemetry_v1_0_sample.json`**

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

- [ ] **Step 2: Write the failing test `libs/eirvah-contracts/tests/test_telemetry.py`**

```python
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from eirvah_contracts.telemetry import (
    SOURCE_PROTOCOL_OPCUA,
    TelemetryPayload,
    TelemetrySource,
    TelemetryTimestamps,
)
from eirvah_contracts.uns import UNSPath

GOLDEN = Path(__file__).parent / "golden" / "telemetry_v1_0_sample.json"


def test_golden_fixture_validates_as_v1() -> None:
    raw = json.loads(GOLDEN.read_text())
    payload = TelemetryPayload.model_validate(raw)
    assert payload.schema_version == "1.0"
    assert payload.value == 23.4
    assert payload.uns_path.measurement == "temperature"
    assert payload.source.protocol == "opcua"


def test_round_trip_through_json_preserves_fixture_semantics() -> None:
    raw = json.loads(GOLDEN.read_text())
    payload = TelemetryPayload.model_validate(raw)
    re_serialised = json.loads(payload.model_dump_json())
    assert re_serialised == raw


def test_unknown_optional_fields_are_accepted() -> None:
    raw = json.loads(GOLDEN.read_text())
    raw["tags"] = {"site_owner": "uniza-it"}
    raw["lineage"] = ["opcua-data-subscriber", "data-converter"]
    TelemetryPayload.model_validate(raw)  # should not raise


def test_missing_required_field_rejected() -> None:
    raw = json.loads(GOLDEN.read_text())
    del raw["timestamps"]
    with pytest.raises(ValueError):
        TelemetryPayload.model_validate(raw)


def test_construct_programmatically() -> None:
    p = TelemetryPayload(
        correlation_id="01HZXC8P9G7Q3M6V0K2T8R5W4A",
        value=22.0,
        value_type="double",
        semantic_type="setpoint.target",
        unit="degC",
        quality="good",
        uns_path=UNSPath(
            enterprise="uniza",
            site="zilina",
            area="factory1",
            line="line_a",
            cell="bottler",
            equipment="setpoint_unit",
            measurement="setpoint_temperature",
        ),
        source=TelemetrySource(
            protocol=SOURCE_PROTOCOL_OPCUA,
            endpoint="opc.tcp://opcua-simulator:4840",
            node_id="ns=2;s=Bottler.Setpoint",
        ),
        timestamps=TelemetryTimestamps(
            source=datetime(2026, 5, 16, 13, 45, 22, 123456, tzinfo=timezone.utc),
            edge_ingress=datetime(2026, 5, 16, 13, 45, 22, 150123, tzinfo=timezone.utc),
            edge_publish=datetime(2026, 5, 16, 13, 45, 22, 152456, tzinfo=timezone.utc),
        ),
    )
    assert p.schema_version == "1.0"
```

- [ ] **Step 3: Run the test (red)**

Run:
```bash
uv run pytest libs/eirvah-contracts/tests/test_telemetry.py -v
```
Expected: ImportError on `eirvah_contracts.telemetry`.

- [ ] **Step 4: Write `libs/eirvah-contracts/src/eirvah_contracts/telemetry.py`**

```python
"""TelemetryPayload v1.0 — the public MQTT payload (spec §4.2)."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict

from eirvah_contracts.signals import Quality, SignalValue, SignalValueType
from eirvah_contracts.ulid import is_valid_correlation_id
from eirvah_contracts.uns import UNSPath

SOURCE_PROTOCOL_OPCUA: Literal["opcua"] = "opcua"


class TelemetrySource(BaseModel):
    """Provenance for a telemetry message: how the value was originally produced."""

    model_config = ConfigDict(extra="forbid")

    protocol: Literal["opcua", "modbus", "siemens_s7"]
    endpoint: str
    node_id: str


class TelemetryTimestamps(BaseModel):
    """Three time-points along the telemetry path (all ISO 8601 UTC)."""

    model_config = ConfigDict(extra="forbid")

    source: datetime          # set by device / simulator
    edge_ingress: datetime    # set by opcua-data-subscriber
    edge_publish: datetime    # set by mqtt-uns-publisher


class TelemetryPayload(BaseModel):
    """Public MQTT payload, schema version 1.0 (spec §4.2).

    Consumers MUST tolerate unknown additional fields within the same major
    version (forward-compatibility). That is what ``extra="allow"`` expresses.
    """

    model_config = ConfigDict(extra="allow")

    schema_version: Literal["1.0"] = "1.0"
    correlation_id: str
    value: SignalValue
    value_type: SignalValueType
    semantic_type: str
    unit: str
    quality: Quality
    uns_path: UNSPath
    source: TelemetrySource
    timestamps: TelemetryTimestamps

    def model_post_init(self, __context: object) -> None:
        if not is_valid_correlation_id(self.correlation_id):
            raise ValueError(f"invalid correlation_id: {self.correlation_id!r}")
```

- [ ] **Step 5: Run the test (green)**

Run:
```bash
uv run pytest libs/eirvah-contracts/tests/test_telemetry.py -v
uv run mypy libs/eirvah-contracts/src
```
Expected: pass.

- [ ] **Step 6: Commit**

```bash
git add libs/eirvah-contracts/src/eirvah_contracts/telemetry.py \
        libs/eirvah-contracts/tests/test_telemetry.py \
        libs/eirvah-contracts/tests/golden/telemetry_v1_0_sample.json
git commit -m "feat(contracts): TelemetryPayload v1.0 + golden fixture (spec §4.2)"
```

---

### Task 8: `eirvah-contracts` — ActuationRequest + result events v1.0 + golden fixtures

Spec §4.3. Three fixtures: request, approve result, reject result.

**Files:**
- Create: `libs/eirvah-contracts/src/eirvah_contracts/actuation.py`
- Create: `libs/eirvah-contracts/tests/test_actuation.py`
- Create: `libs/eirvah-contracts/tests/golden/actuation_request_v1_0_sample.json`
- Create: `libs/eirvah-contracts/tests/golden/actuation_approve_v1_0_sample.json`
- Create: `libs/eirvah-contracts/tests/golden/actuation_reject_v1_0_sample.json`

- [ ] **Step 1: Write `libs/eirvah-contracts/tests/golden/actuation_request_v1_0_sample.json`**

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

- [ ] **Step 2: Write `libs/eirvah-contracts/tests/golden/actuation_approve_v1_0_sample.json`**

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
  "deadline": "2026-05-16T13:45:30.000000Z",
  "decision": "approve",
  "written_at": "2026-05-16T13:45:25.500000Z"
}
```

- [ ] **Step 3: Write `libs/eirvah-contracts/tests/golden/actuation_reject_v1_0_sample.json`**

```json
{
  "schema_version": "1.0",
  "correlation_id": "01HZXC8P9G7Q3M6V0K2T8R5W4A",
  "requester": "decision-agent-stub",
  "target_uns_topic": "uniza/zilina/factory1/line_a/bottler/setpoint_unit/setpoint_temperature",
  "requested_value": 99.0,
  "value_type": "double",
  "reason": "telemetry threshold breach",
  "requested_at": "2026-05-16T13:45:25.000000Z",
  "deadline": "2026-05-16T13:45:30.000000Z",
  "decision": "reject",
  "rejection_reason": "value 99.0 outside policy range [15.0, 30.0]"
}
```

- [ ] **Step 4: Write the failing test `libs/eirvah-contracts/tests/test_actuation.py`**

```python
import json
from pathlib import Path

import pytest

from eirvah_contracts.actuation import (
    ActuationApproveResult,
    ActuationRejectResult,
    ActuationRequest,
)

GOLDEN_DIR = Path(__file__).parent / "golden"


def _load(name: str) -> dict:
    return json.loads((GOLDEN_DIR / name).read_text())


def test_request_golden_validates() -> None:
    req = ActuationRequest.model_validate(_load("actuation_request_v1_0_sample.json"))
    assert req.requested_value == 22.0
    assert req.requester == "decision-agent-stub"


def test_request_round_trip_through_json() -> None:
    raw = _load("actuation_request_v1_0_sample.json")
    req = ActuationRequest.model_validate(raw)
    assert json.loads(req.model_dump_json()) == raw


def test_approve_result_golden_validates() -> None:
    res = ActuationApproveResult.model_validate(
        _load("actuation_approve_v1_0_sample.json")
    )
    assert res.decision == "approve"
    assert res.written_at is not None


def test_reject_result_golden_validates() -> None:
    res = ActuationRejectResult.model_validate(
        _load("actuation_reject_v1_0_sample.json")
    )
    assert res.decision == "reject"
    assert "outside policy range" in res.rejection_reason


def test_approve_rejects_decision_other_than_approve() -> None:
    raw = _load("actuation_approve_v1_0_sample.json")
    raw["decision"] = "reject"
    with pytest.raises(ValueError):
        ActuationApproveResult.model_validate(raw)


def test_reject_rejects_decision_other_than_reject() -> None:
    raw = _load("actuation_reject_v1_0_sample.json")
    raw["decision"] = "approve"
    with pytest.raises(ValueError):
        ActuationRejectResult.model_validate(raw)


def test_request_rejects_invalid_correlation_id() -> None:
    raw = _load("actuation_request_v1_0_sample.json")
    raw["correlation_id"] = "not-a-ulid"
    with pytest.raises(ValueError):
        ActuationRequest.model_validate(raw)


def test_request_rejects_malformed_target_uns_topic() -> None:
    raw = _load("actuation_request_v1_0_sample.json")
    raw["target_uns_topic"] = "too/few/segments"
    with pytest.raises(ValueError):
        ActuationRequest.model_validate(raw)
```

- [ ] **Step 5: Run the test (red)**

Run:
```bash
uv run pytest libs/eirvah-contracts/tests/test_actuation.py -v
```
Expected: ImportError on `eirvah_contracts.actuation`.

- [ ] **Step 6: Write `libs/eirvah-contracts/src/eirvah_contracts/actuation.py`**

```python
"""ActuationRequest + result events (spec §4.3)."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, field_validator

from eirvah_contracts.signals import SignalValue, SignalValueType
from eirvah_contracts.ulid import is_valid_correlation_id
from eirvah_contracts.uns import parse_uns_topic


class ActuationRequest(BaseModel):
    """AMQP payload on ``eirvah.actuation.requests`` (spec §4.3)."""

    model_config = ConfigDict(extra="allow")

    schema_version: Literal["1.0"] = "1.0"
    correlation_id: str
    requester: str
    target_uns_topic: str
    requested_value: SignalValue
    value_type: SignalValueType
    reason: str
    requested_at: datetime
    deadline: datetime | None = None

    @field_validator("correlation_id")
    @classmethod
    def _validate_correlation_id(cls, value: str) -> str:
        if not is_valid_correlation_id(value):
            raise ValueError(f"invalid correlation_id: {value!r}")
        return value

    @field_validator("target_uns_topic")
    @classmethod
    def _validate_target_topic(cls, value: str) -> str:
        # Will raise ValueError if invalid; we keep the raw string but verify shape.
        parse_uns_topic(value)
        return value


class ActuationApproveResult(ActuationRequest):
    """Approve event on the AMQP results exchange."""

    decision: Literal["approve"]
    written_at: datetime | None = None


class ActuationRejectResult(ActuationRequest):
    """Reject event on the AMQP results exchange."""

    decision: Literal["reject"]
    rejection_reason: str
```

- [ ] **Step 7: Run the test (green)**

Run:
```bash
uv run pytest libs/eirvah-contracts/tests/test_actuation.py -v
uv run mypy libs/eirvah-contracts/src
```
Expected: pass.

- [ ] **Step 8: Commit**

```bash
git add libs/eirvah-contracts/src/eirvah_contracts/actuation.py \
        libs/eirvah-contracts/tests/test_actuation.py \
        libs/eirvah-contracts/tests/golden/actuation_*.json
git commit -m "feat(contracts): ActuationRequest + approve/reject results (spec §4.3)"
```

---

### Task 9: `eirvah-bus` — package skeleton and NATS client wrapper

**Files:**
- Create: `libs/eirvah-bus/pyproject.toml`
- Create: `libs/eirvah-bus/src/eirvah_bus/__init__.py`
- Create: `libs/eirvah-bus/src/eirvah_bus/client.py`
- Create: `libs/eirvah-bus/tests/__init__.py`
- Create: `libs/eirvah-bus/tests/test_client.py`

- [ ] **Step 1: Write `libs/eirvah-bus/pyproject.toml`**

```toml
[project]
name = "eirvah-bus"
version = "0.0.0"
description = "NATS client wrappers for EirVah edge services."
requires-python = ">=3.12"
license = { text = "Apache-2.0" }
dependencies = [
    "nats-py>=2.7",
    "eirvah-contracts",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/eirvah_bus"]
```

`nats-py` is Apache-2.0. License-clean.

- [ ] **Step 2: Write `libs/eirvah-bus/src/eirvah_bus/__init__.py`**

```python
"""NATS helpers shared across all EirVah edge services."""
```

- [ ] **Step 3: Write the failing test `libs/eirvah-bus/tests/test_client.py`**

```python
from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from eirvah_bus.client import BusClient


@pytest.mark.asyncio
async def test_connect_calls_nats_connect_with_servers() -> None:
    fake_nc = AsyncMock()
    with patch("eirvah_bus.client.nats.connect", AsyncMock(return_value=fake_nc)) as conn:
        client = BusClient(servers=["nats://nats:4222"])
        await client.connect()
        conn.assert_awaited_once()
        assert client.nc is fake_nc


@pytest.mark.asyncio
async def test_close_drains_underlying_connection() -> None:
    fake_nc = AsyncMock()
    with patch("eirvah_bus.client.nats.connect", AsyncMock(return_value=fake_nc)):
        client = BusClient(servers=["nats://nats:4222"])
        await client.connect()
        await client.close()
        fake_nc.drain.assert_awaited_once()


@pytest.mark.asyncio
async def test_connect_is_idempotent() -> None:
    fake_nc = AsyncMock()
    with patch("eirvah_bus.client.nats.connect", AsyncMock(return_value=fake_nc)) as conn:
        client = BusClient(servers=["nats://nats:4222"])
        await client.connect()
        await client.connect()
        assert conn.await_count == 1
```

Add `libs/eirvah-bus/tests/__init__.py` (empty).

- [ ] **Step 4: Add `pytest-asyncio` to dev deps and sync**

Append to root `pyproject.toml` under `[tool.uv]`:

```toml
[dependency-groups]
dev = [
    "pytest>=8.3",
    "pytest-asyncio>=0.24",
    "ruff>=0.6",
    "mypy>=1.11",
]
```

Then:
```bash
uv sync
```
Expected: pytest-asyncio installed.

- [ ] **Step 5: Run the test (red)**

Run:
```bash
uv run pytest libs/eirvah-bus/tests/test_client.py -v
```
Expected: ImportError on `eirvah_bus.client`.

- [ ] **Step 6: Write `libs/eirvah-bus/src/eirvah_bus/client.py`**

```python
"""Lifecycle wrapper around a NATS client connection."""

from __future__ import annotations

from collections.abc import Sequence

import nats
from nats.aio.client import Client as NATSClient


class BusClient:
    """Owns a single NATS connection for the life of a service process."""

    def __init__(self, servers: Sequence[str], name: str | None = None) -> None:
        self._servers: list[str] = list(servers)
        self._name = name
        self._nc: NATSClient | None = None

    @property
    def nc(self) -> NATSClient:
        if self._nc is None:
            raise RuntimeError("BusClient.connect() must be awaited before use")
        return self._nc

    @property
    def connected(self) -> bool:
        return self._nc is not None and self._nc.is_connected

    async def connect(self) -> None:
        """Establish the NATS connection; idempotent."""
        if self._nc is not None:
            return
        self._nc = await nats.connect(servers=self._servers, name=self._name)

    async def close(self) -> None:
        """Drain in-flight messages and close the connection."""
        if self._nc is None:
            return
        await self._nc.drain()
        self._nc = None
```

- [ ] **Step 7: Run the test (green)**

Run:
```bash
uv run pytest libs/eirvah-bus/tests/test_client.py -v
uv run mypy libs/eirvah-bus/src
```
Expected: pass.

- [ ] **Step 8: Commit**

```bash
git add libs/eirvah-bus pyproject.toml uv.lock
git commit -m "feat(bus): NATS client wrapper with idempotent connect/close"
```

---

### Task 10: `eirvah-bus` — request-reply with timeout and correlation header

**Files:**
- Create: `libs/eirvah-bus/src/eirvah_bus/request_reply.py`
- Create: `libs/eirvah-bus/tests/test_request_reply.py`

- [ ] **Step 1: Write the failing test `libs/eirvah-bus/tests/test_request_reply.py`**

```python
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from eirvah_bus.request_reply import (
    BUS_HEADER_CORRELATION_ID,
    RequestTimeout,
    request_reply,
)


class _FakeMsg:
    def __init__(self, data: bytes, headers: dict[str, str] | None = None) -> None:
        self.data = data
        self.headers = headers or {}


@pytest.mark.asyncio
async def test_request_reply_propagates_payload_and_header() -> None:
    nc = AsyncMock()
    nc.request = AsyncMock(return_value=_FakeMsg(b'{"ok": true}'))
    correlation_id = "01HZXC8P9G7Q3M6V0K2T8R5W4A"
    payload = b'{"value": 42}'

    reply = await request_reply(
        nc=nc,
        subject="uns.work.convert",
        payload=payload,
        correlation_id=correlation_id,
        timeout_s=1.0,
    )

    nc.request.assert_awaited_once()
    args, kwargs = nc.request.call_args
    assert args[0] == "uns.work.convert"
    assert args[1] == payload
    assert kwargs["timeout"] == 1.0
    assert kwargs["headers"][BUS_HEADER_CORRELATION_ID] == correlation_id
    assert reply.data == b'{"ok": true}'


@pytest.mark.asyncio
async def test_request_reply_translates_asyncio_timeout() -> None:
    nc = AsyncMock()
    nc.request = AsyncMock(side_effect=asyncio.TimeoutError())
    with pytest.raises(RequestTimeout):
        await request_reply(
            nc=nc,
            subject="uns.work.convert",
            payload=b"{}",
            correlation_id="01HZXC8P9G7Q3M6V0K2T8R5W4A",
            timeout_s=0.1,
        )
```

- [ ] **Step 2: Run the test (red)**

Run:
```bash
uv run pytest libs/eirvah-bus/tests/test_request_reply.py -v
```
Expected: ImportError on `eirvah_bus.request_reply`.

- [ ] **Step 3: Write `libs/eirvah-bus/src/eirvah_bus/request_reply.py`**

```python
"""Request-reply helper with per-call timeout and correlation-ID propagation."""

from __future__ import annotations

import asyncio

from nats.aio.client import Client as NATSClient
from nats.aio.msg import Msg

#: Header carrying the ULID correlation ID across every NATS hop (spec §4.4).
BUS_HEADER_CORRELATION_ID = "X-Correlation-Id"


class RequestTimeout(TimeoutError):
    """Raised when a NATS request-reply call exceeds its per-call timeout."""


async def request_reply(
    *,
    nc: NATSClient,
    subject: str,
    payload: bytes,
    correlation_id: str,
    timeout_s: float,
) -> Msg:
    """Send a NATS request and await a reply, with timeout + correlation header.

    Raises ``RequestTimeout`` (a subclass of ``TimeoutError``) on timeout so
    callers can distinguish bus-timeouts from other ``TimeoutError`` sources.
    """
    headers = {BUS_HEADER_CORRELATION_ID: correlation_id}
    try:
        return await nc.request(subject, payload, timeout=timeout_s, headers=headers)
    except asyncio.TimeoutError as exc:
        raise RequestTimeout(
            f"NATS request to {subject!r} timed out after {timeout_s}s"
        ) from exc
```

- [ ] **Step 4: Run the test (green)**

Run:
```bash
uv run pytest libs/eirvah-bus/tests/test_request_reply.py -v
uv run mypy libs/eirvah-bus/src
```
Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add libs/eirvah-bus/src/eirvah_bus/request_reply.py libs/eirvah-bus/tests/test_request_reply.py
git commit -m "feat(bus): request-reply wrapper with timeout and correlation header"
```

---

### Task 11: `eirvah-bus` — queue-group consumer

**Files:**
- Create: `libs/eirvah-bus/src/eirvah_bus/consumer.py`
- Create: `libs/eirvah-bus/tests/test_consumer.py`

- [ ] **Step 1: Write the failing test `libs/eirvah-bus/tests/test_consumer.py`**

```python
from __future__ import annotations

from collections.abc import Awaitable, Callable
from unittest.mock import AsyncMock

import pytest

from eirvah_bus.consumer import subscribe_queue_group


@pytest.mark.asyncio
async def test_subscribe_queue_group_registers_handler() -> None:
    nc = AsyncMock()
    captured: list[bytes] = []

    async def handler(msg) -> None:  # type: ignore[no-untyped-def]
        captured.append(msg.data)

    await subscribe_queue_group(
        nc=nc,
        subject="uns.work.convert",
        queue="uns.work.convert",
        handler=handler,
    )

    nc.subscribe.assert_awaited_once_with(
        "uns.work.convert",
        queue="uns.work.convert",
        cb=handler,
    )


@pytest.mark.asyncio
async def test_subscribe_queue_group_defaults_queue_to_subject() -> None:
    nc = AsyncMock()

    async def handler(msg) -> None:  # type: ignore[no-untyped-def]
        return None

    await subscribe_queue_group(nc=nc, subject="act.work.validate", handler=handler)

    nc.subscribe.assert_awaited_once_with(
        "act.work.validate",
        queue="act.work.validate",
        cb=handler,
    )
```

- [ ] **Step 2: Run the test (red)**

Run:
```bash
uv run pytest libs/eirvah-bus/tests/test_consumer.py -v
```
Expected: ImportError on `eirvah_bus.consumer`.

- [ ] **Step 3: Write `libs/eirvah-bus/src/eirvah_bus/consumer.py`**

```python
"""Queue-group consumer helper.

NATS queue groups load-balance messages across all subscribers in the group.
By convention we name the queue the same as the subject — so scaling a worker
to N replicas just works (spec §4.4).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from nats.aio.client import Client as NATSClient
from nats.aio.msg import Msg

Handler = Callable[[Msg], Awaitable[None]]


async def subscribe_queue_group(
    *,
    nc: NATSClient,
    subject: str,
    handler: Handler,
    queue: str | None = None,
) -> None:
    """Subscribe *handler* to *subject* in a NATS queue group.

    If *queue* is omitted, it defaults to *subject* — the EirVah convention.
    """
    await nc.subscribe(subject, queue=queue or subject, cb=handler)
```

- [ ] **Step 4: Run the test (green)**

Run:
```bash
uv run pytest libs/eirvah-bus/tests/test_consumer.py -v
uv run mypy libs/eirvah-bus/src
```
Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add libs/eirvah-bus/src/eirvah_bus/consumer.py libs/eirvah-bus/tests/test_consumer.py
git commit -m "feat(bus): queue-group consumer helper"
```

---

### Task 12: `eirvah-observability` — package skeleton and Prometheus metric factories

**Files:**
- Create: `libs/eirvah-observability/pyproject.toml`
- Create: `libs/eirvah-observability/src/eirvah_observability/__init__.py`
- Create: `libs/eirvah-observability/src/eirvah_observability/metrics.py`
- Create: `libs/eirvah-observability/tests/__init__.py`
- Create: `libs/eirvah-observability/tests/test_metrics.py`

- [ ] **Step 1: Write `libs/eirvah-observability/pyproject.toml`**

```toml
[project]
name = "eirvah-observability"
version = "0.0.0"
description = "Prometheus, structured logging, and /healthz /readyz /metrics ASGI helpers."
requires-python = ">=3.12"
license = { text = "Apache-2.0" }
dependencies = [
    "prometheus-client>=0.20",
    "structlog>=24.4",
    "starlette>=0.40",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/eirvah_observability"]
```

`prometheus-client` is Apache-2.0; `structlog` MIT/Apache-2.0; `starlette` BSD-3-Clause. License-clean.

- [ ] **Step 2: Write `libs/eirvah-observability/src/eirvah_observability/__init__.py`**

```python
"""Observability helpers — metrics, logging, health endpoints."""
```

- [ ] **Step 3: Write the failing test `libs/eirvah-observability/tests/test_metrics.py`**

```python
from __future__ import annotations

import pytest
from prometheus_client import CollectorRegistry

from eirvah_observability.metrics import (
    EIRVAH_METRIC_PREFIX,
    make_counter,
    make_gauge,
    make_histogram,
)


def test_counter_carries_eirvah_prefix() -> None:
    reg = CollectorRegistry()
    c = make_counter("test_counter", "doc", labelnames=["stage"], registry=reg)
    c.labels(stage="convert").inc()
    assert reg.get_sample_value(f"{EIRVAH_METRIC_PREFIX}_test_counter_total", {"stage": "convert"}) == 1.0


def test_gauge_carries_eirvah_prefix() -> None:
    reg = CollectorRegistry()
    g = make_gauge("test_gauge_celsius", "doc", labelnames=[], registry=reg)
    g.set(23.4)
    assert reg.get_sample_value(f"{EIRVAH_METRIC_PREFIX}_test_gauge_celsius") == 23.4


def test_histogram_default_buckets_present() -> None:
    reg = CollectorRegistry()
    h = make_histogram("test_latency_seconds", "doc", labelnames=[], registry=reg)
    h.observe(0.5)
    assert reg.get_sample_value(f"{EIRVAH_METRIC_PREFIX}_test_latency_seconds_count") == 1.0


def test_make_counter_rejects_name_with_prefix_duplication() -> None:
    reg = CollectorRegistry()
    with pytest.raises(ValueError):
        make_counter("eirvah_double_prefix", "doc", labelnames=[], registry=reg)
```

Add `libs/eirvah-observability/tests/__init__.py` (empty).

- [ ] **Step 4: Run the test (red)**

Run:
```bash
uv sync
uv run pytest libs/eirvah-observability/tests/test_metrics.py -v
```
Expected: ImportError on `eirvah_observability.metrics`.

- [ ] **Step 5: Write `libs/eirvah-observability/src/eirvah_observability/metrics.py`**

```python
"""Prometheus metric factories with a uniform ``eirvah_`` namespace.

Every metric in EirVah is created through one of these factories so prefix,
default buckets, and registry handling are consistent across services.
"""

from __future__ import annotations

from collections.abc import Sequence

from prometheus_client import Counter, Gauge, Histogram
from prometheus_client.registry import CollectorRegistry

EIRVAH_METRIC_PREFIX = "eirvah"

# Latency buckets cover the ranges relevant to the slice: sub-ms NATS hops
# through to multi-second timeouts.
DEFAULT_LATENCY_BUCKETS: tuple[float, ...] = (
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
)


def _full_name(name: str) -> str:
    if name.startswith(f"{EIRVAH_METRIC_PREFIX}_"):
        raise ValueError(
            f"metric name {name!r} already starts with {EIRVAH_METRIC_PREFIX!r}; "
            "let the factory add the prefix"
        )
    return f"{EIRVAH_METRIC_PREFIX}_{name}"


def make_counter(
    name: str,
    documentation: str,
    *,
    labelnames: Sequence[str],
    registry: CollectorRegistry | None = None,
) -> Counter:
    return Counter(_full_name(name), documentation, labelnames=labelnames, registry=registry)


def make_gauge(
    name: str,
    documentation: str,
    *,
    labelnames: Sequence[str],
    registry: CollectorRegistry | None = None,
) -> Gauge:
    return Gauge(_full_name(name), documentation, labelnames=labelnames, registry=registry)


def make_histogram(
    name: str,
    documentation: str,
    *,
    labelnames: Sequence[str],
    buckets: Sequence[float] = DEFAULT_LATENCY_BUCKETS,
    registry: CollectorRegistry | None = None,
) -> Histogram:
    return Histogram(
        _full_name(name),
        documentation,
        labelnames=labelnames,
        buckets=buckets,
        registry=registry,
    )
```

- [ ] **Step 6: Run the test (green)**

Run:
```bash
uv run pytest libs/eirvah-observability/tests/test_metrics.py -v
uv run mypy libs/eirvah-observability/src
```
Expected: pass.

- [ ] **Step 7: Commit**

```bash
git add libs/eirvah-observability pyproject.toml uv.lock
git commit -m "feat(observability): Prometheus metric factories with eirvah_ prefix"
```

---

### Task 13: `eirvah-observability` — structlog config

**Files:**
- Create: `libs/eirvah-observability/src/eirvah_observability/logging.py`
- Create: `libs/eirvah-observability/tests/test_logging.py`

- [ ] **Step 1: Write the failing test `libs/eirvah-observability/tests/test_logging.py`**

```python
from __future__ import annotations

import io
import json
import logging

import structlog

from eirvah_observability.logging import configure_logging, bind_correlation_id


def test_configure_logging_emits_json_to_stdout() -> None:
    buffer = io.StringIO()
    configure_logging(level="INFO", stream=buffer)
    log = structlog.get_logger("test")
    log.info("hello", k=1)

    line = buffer.getvalue().strip().splitlines()[-1]
    parsed = json.loads(line)
    assert parsed["event"] == "hello"
    assert parsed["k"] == 1
    assert parsed["level"] == "info"
    assert parsed["logger"] == "test"
    assert "timestamp" in parsed


def test_bind_correlation_id_attaches_field() -> None:
    buffer = io.StringIO()
    configure_logging(level="INFO", stream=buffer)
    bind_correlation_id("01HZXC8P9G7Q3M6V0K2T8R5W4A")
    structlog.get_logger("svc").info("event")

    parsed = json.loads(buffer.getvalue().strip().splitlines()[-1])
    assert parsed["correlation_id"] == "01HZXC8P9G7Q3M6V0K2T8R5W4A"


def test_level_filtering_drops_debug_when_info() -> None:
    buffer = io.StringIO()
    configure_logging(level="INFO", stream=buffer)
    structlog.get_logger("svc").debug("hidden")
    assert buffer.getvalue() == ""
```

- [ ] **Step 2: Run the test (red)**

Run:
```bash
uv run pytest libs/eirvah-observability/tests/test_logging.py -v
```
Expected: ImportError on `eirvah_observability.logging`.

- [ ] **Step 3: Write `libs/eirvah-observability/src/eirvah_observability/logging.py`**

```python
"""Uniform structlog configuration for every EirVah service.

Every log line is JSON. Every line includes a UTC ISO 8601 timestamp, a level,
a logger name, the event, and any structured context — including the
correlation_id when one is bound to the current context.
"""

from __future__ import annotations

import logging
import sys
from typing import IO, Any

import structlog


def configure_logging(
    level: str = "INFO",
    stream: IO[str] | None = None,
) -> None:
    """Configure stdlib + structlog to emit JSON lines to ``stream``.

    ``stream`` defaults to ``sys.stdout`` so Kubernetes captures the output
    via ``kubectl logs`` without further wiring.
    """
    out = stream if stream is not None else sys.stdout

    log_level = getattr(logging, level.upper())
    logging.basicConfig(stream=out, level=log_level, format="%(message)s", force=True)

    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            _add_logger_name,
            structlog.processors.JSONRenderer(),
        ],
        cache_logger_on_first_use=True,
    )


def bind_correlation_id(correlation_id: str) -> None:
    """Bind ``correlation_id`` to all subsequent log calls in this context."""
    structlog.contextvars.bind_contextvars(correlation_id=correlation_id)


def clear_correlation_id() -> None:
    """Clear any previously bound correlation_id."""
    structlog.contextvars.unbind_contextvars("correlation_id")


def _add_logger_name(_logger: Any, _method: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    name = event_dict.pop("_record", None)
    if name is not None and hasattr(name, "name"):
        event_dict.setdefault("logger", name.name)
    elif "logger" not in event_dict and "_logger" in event_dict:
        event_dict["logger"] = event_dict.pop("_logger")
    return event_dict
```

- [ ] **Step 4: Run the test (green)**

Run:
```bash
uv run pytest libs/eirvah-observability/tests/test_logging.py -v
uv run mypy libs/eirvah-observability/src
```
Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add libs/eirvah-observability/src/eirvah_observability/logging.py \
        libs/eirvah-observability/tests/test_logging.py
git commit -m "feat(observability): JSON structlog config with correlation_id binding"
```

---

### Task 14: `eirvah-observability` — `/healthz` `/readyz` `/metrics` ASGI app

**Files:**
- Create: `libs/eirvah-observability/src/eirvah_observability/health.py`
- Create: `libs/eirvah-observability/tests/test_health.py`

- [ ] **Step 1: Write the failing test `libs/eirvah-observability/tests/test_health.py`**

```python
from __future__ import annotations

import pytest
from prometheus_client import CollectorRegistry
from starlette.testclient import TestClient

from eirvah_observability.health import HealthApp
from eirvah_observability.metrics import make_counter


def _readiness_true() -> bool:
    return True


def _readiness_false() -> bool:
    return False


def test_healthz_returns_200_when_alive() -> None:
    app = HealthApp(is_ready=_readiness_true)
    client = TestClient(app.asgi)
    assert client.get("/healthz").status_code == 200


def test_readyz_returns_200_when_ready() -> None:
    app = HealthApp(is_ready=_readiness_true)
    client = TestClient(app.asgi)
    assert client.get("/readyz").status_code == 200


def test_readyz_returns_503_when_not_ready() -> None:
    app = HealthApp(is_ready=_readiness_false)
    client = TestClient(app.asgi)
    assert client.get("/readyz").status_code == 503


def test_metrics_serves_prometheus_exposition() -> None:
    reg = CollectorRegistry()
    c = make_counter("health_app_test", "doc", labelnames=[], registry=reg)
    c.inc()
    app = HealthApp(is_ready=_readiness_true, registry=reg)
    client = TestClient(app.asgi)
    resp = client.get("/metrics")
    assert resp.status_code == 200
    assert "eirvah_health_app_test_total" in resp.text


def test_unknown_path_returns_404() -> None:
    app = HealthApp(is_ready=_readiness_true)
    client = TestClient(app.asgi)
    assert client.get("/whatever").status_code == 404
```

Update `[dependency-groups].dev` in root `pyproject.toml` to add the test client dependency:

```toml
[dependency-groups]
dev = [
    "pytest>=8.3",
    "pytest-asyncio>=0.24",
    "httpx>=0.27",         # needed by starlette.testclient
    "ruff>=0.6",
    "mypy>=1.11",
]
```

Then:
```bash
uv sync
```

- [ ] **Step 2: Run the test (red)**

Run:
```bash
uv run pytest libs/eirvah-observability/tests/test_health.py -v
```
Expected: ImportError on `eirvah_observability.health`.

- [ ] **Step 3: Write `libs/eirvah-observability/src/eirvah_observability/health.py`**

```python
"""ASGI app exposing ``/healthz``, ``/readyz``, and ``/metrics``."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from prometheus_client.registry import REGISTRY, CollectorRegistry
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import PlainTextResponse, Response
from starlette.routing import Route

if TYPE_CHECKING:
    from starlette.types import ASGIApp


class HealthApp:
    """Bundle of the three operational endpoints every EirVah service exposes.

    ``is_ready`` is a callable so services can plug in their own readiness
    semantics (e.g. NATS connected, OPC UA session up).
    """

    def __init__(
        self,
        *,
        is_ready: Callable[[], bool],
        registry: CollectorRegistry = REGISTRY,
    ) -> None:
        self._is_ready = is_ready
        self._registry = registry
        self._app = Starlette(
            routes=[
                Route("/healthz", self._healthz, methods=["GET"]),
                Route("/readyz", self._readyz, methods=["GET"]),
                Route("/metrics", self._metrics, methods=["GET"]),
            ]
        )

    @property
    def asgi(self) -> ASGIApp:
        return self._app

    async def _healthz(self, _request: Request) -> Response:
        return PlainTextResponse("ok")

    async def _readyz(self, _request: Request) -> Response:
        if self._is_ready():
            return PlainTextResponse("ready")
        return PlainTextResponse("not ready", status_code=503)

    async def _metrics(self, _request: Request) -> Response:
        return Response(
            content=generate_latest(self._registry),
            media_type=CONTENT_TYPE_LATEST,
        )
```

- [ ] **Step 4: Run the test (green)**

Run:
```bash
uv run pytest libs/eirvah-observability/tests/test_health.py -v
uv run mypy libs/eirvah-observability/src
```
Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add libs/eirvah-observability/src/eirvah_observability/health.py \
        libs/eirvah-observability/tests/test_health.py \
        pyproject.toml uv.lock
git commit -m "feat(observability): /healthz /readyz /metrics ASGI app"
```

---

### Task 15: `opcua-simulator` — service scaffolding + config

**Files:**
- Create: `services/opcua-simulator/pyproject.toml`
- Create: `services/opcua-simulator/Dockerfile`
- Create: `services/opcua-simulator/src/opcua_simulator/__init__.py`
- Create: `services/opcua-simulator/src/opcua_simulator/__main__.py`
- Create: `services/opcua-simulator/src/opcua_simulator/config.py`
- Create: `services/opcua-simulator/tests/__init__.py`
- Create: `services/opcua-simulator/tests/test_config.py`

- [ ] **Step 1: Write `services/opcua-simulator/pyproject.toml`**

```toml
[project]
name = "opcua-simulator"
version = "0.0.0"
description = "EirVah bottling-line OPC UA simulator (spec §9)."
requires-python = ">=3.12"
license = { text = "Apache-2.0" }
dependencies = [
    "asyncua>=1.1",
    "pydantic>=2.8",
    "pydantic-settings>=2.5",
    "pyyaml>=6.0",
    "uvicorn>=0.30",
    "eirvah-contracts",
    "eirvah-observability",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/opcua_simulator"]
```

`asyncua` is LGPL-3.0 — OSI-approved. `pyyaml` MIT. `uvicorn` BSD-3-Clause. License-clean.

- [ ] **Step 2: Write `services/opcua-simulator/Dockerfile`**

```dockerfile
# syntax=docker/dockerfile:1.7
FROM python:3.12-slim AS builder
WORKDIR /workspace
RUN pip install --no-cache-dir uv==0.4.20
COPY pyproject.toml uv.lock /workspace/
COPY libs /workspace/libs
COPY services/opcua-simulator /workspace/services/opcua-simulator
RUN uv sync --frozen --no-dev --package opcua-simulator

FROM gcr.io/distroless/python3-debian12 AS runtime
WORKDIR /app
COPY --from=builder /workspace/.venv /app/.venv
COPY --from=builder /workspace/libs /app/libs
COPY --from=builder /workspace/services/opcua-simulator/src /app/src
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONPATH="/app/src" \
    PYTHONDONTWRITEBYTECODE=1
USER nonroot:nonroot
EXPOSE 4840 8080
ENTRYPOINT ["/app/.venv/bin/python", "-m", "opcua_simulator"]
```

- [ ] **Step 3: Write `services/opcua-simulator/src/opcua_simulator/__init__.py`**

```python
"""EirVah bottling-line OPC UA simulator (spec §9)."""
```

- [ ] **Step 4: Write the failing test `services/opcua-simulator/tests/test_config.py`**

```python
from __future__ import annotations

import os

from opcua_simulator.config import SimulatorSettings


def test_defaults_are_safe(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    for var in (
        "OPCUA_SIMULATOR_ENDPOINT",
        "OPCUA_SIMULATOR_TICK_RATE_MS",
        "OPCUA_SIMULATOR_SEED",
        "OPCUA_SIMULATOR_ADDRESS_SPACE_PATH",
        "OPCUA_SIMULATOR_HTTP_PORT",
        "OPCUA_SIMULATOR_HOT_SPIKE_PROBABILITY",
    ):
        monkeypatch.delenv(var, raising=False)
    s = SimulatorSettings()
    assert s.endpoint == "opc.tcp://0.0.0.0:4840/eirvah/simulator"
    assert s.tick_rate_ms == 100
    assert s.seed == 0
    assert s.http_port == 8080
    assert s.hot_spike_probability == 0.0
    assert s.address_space_path.name == "opcua-address-space.yaml"


def test_env_vars_override(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("OPCUA_SIMULATOR_SEED", "1234")
    monkeypatch.setenv("OPCUA_SIMULATOR_TICK_RATE_MS", "50")
    s = SimulatorSettings()
    assert s.seed == 1234
    assert s.tick_rate_ms == 50
```

Add `services/opcua-simulator/tests/__init__.py` (empty).

- [ ] **Step 5: Run the test (red)**

Run:
```bash
uv sync
uv run pytest services/opcua-simulator/tests/test_config.py -v
```
Expected: ImportError on `opcua_simulator.config`.

- [ ] **Step 6: Write `services/opcua-simulator/src/opcua_simulator/config.py`**

```python
"""Environment-driven settings for the OPC UA simulator."""

from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class SimulatorSettings(BaseSettings):
    """All knobs the simulator exposes via env (spec §9.4 + ConfigMap §7.1)."""

    model_config = SettingsConfigDict(
        env_prefix="OPCUA_SIMULATOR_",
        env_file=None,
        extra="ignore",
    )

    endpoint: str = "opc.tcp://0.0.0.0:4840/eirvah/simulator"
    tick_rate_ms: int = Field(default=100, ge=10, le=5000)
    seed: int = 0
    address_space_path: Path = Path("/etc/opcua-simulator/opcua-address-space.yaml")
    http_port: int = Field(default=8080, ge=1024, le=65535)
    hot_spike_probability: float = Field(default=0.0, ge=0.0, le=1.0)
    motor_fault_probability: float = Field(default=0.0, ge=0.0, le=1.0)
    log_level: str = "INFO"
```

- [ ] **Step 7: Write a stub `services/opcua-simulator/src/opcua_simulator/__main__.py`**

```python
"""Entry point. Full wiring happens in Task 25; this stub keeps the package runnable."""

from __future__ import annotations

import asyncio


async def main() -> None:
    raise SystemExit(
        "opcua-simulator entry point is implemented in Task 25 of Plan 1; "
        "this stub exists so the package is importable."
    )


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
```

- [ ] **Step 8: Run the test (green)**

Run:
```bash
uv run pytest services/opcua-simulator/tests/test_config.py -v
uv run mypy services/opcua-simulator/src
```
Expected: pass.

- [ ] **Step 9: Commit**

```bash
git add services/opcua-simulator pyproject.toml uv.lock
git commit -m "chore(simulator): scaffolding, Dockerfile, env-driven settings"
```

---

### Task 16: `opcua-simulator` — address-space loader + sample config

**Files:**
- Create: `config/opcua-address-space.yaml`
- Create: `services/opcua-simulator/src/opcua_simulator/address_space.py`
- Create: `services/opcua-simulator/tests/test_address_space.py`

- [ ] **Step 1: Write `config/opcua-address-space.yaml`** (the bottling-line model from spec §9.1)

```yaml
# Bottling-line address space (spec §9.1).
# Hierarchy maps directly to the ISA-95 UNS path the contextualizer expects.
namespace: "https://eirvah.uniza/zilina/factory1"
uns_defaults:
  enterprise: uniza
  site: zilina
  area: factory1
  line: line_a
equipments:
  - name: bottler
    nodes:
      - id: TemperatureSensor01.Temperature
        kind: measurement
        cell: bottler
        equipment: temperature_sensor_01
        measurement: temperature
        value_type: double
        unit: degC
        initial: 22.0
        semantic_type: temperature.celsius
        dynamics: temperature

      - id: ThroughputMeter01.Throughput
        kind: measurement
        cell: bottler
        equipment: throughput_meter_01
        measurement: throughput
        value_type: double
        unit: bottle/s
        initial: 0.0
        semantic_type: flow.bps
        dynamics: throughput

      - id: Motor01.State
        kind: measurement
        cell: bottler
        equipment: motor_01
        measurement: state
        value_type: int64
        unit: dimensionless
        initial: 0
        semantic_type: state.enum
        dynamics: motor_state

      - id: Motor01.Rpm
        kind: measurement
        cell: bottler
        equipment: motor_01
        measurement: rpm
        value_type: double
        unit: rpm
        initial: 0.0
        semantic_type: speed.rpm
        dynamics: motor_rpm

      - id: SetpointUnit.SetpointTemperature
        kind: setpoint
        cell: bottler
        equipment: setpoint_unit
        measurement: setpoint_temperature
        value_type: double
        unit: degC
        initial: 22.0
        semantic_type: setpoint.target
        policy:
          min: 15.0
          max: 30.0
```

- [ ] **Step 2: Write the failing test `services/opcua-simulator/tests/test_address_space.py`**

```python
from __future__ import annotations

from pathlib import Path

import pytest

from opcua_simulator.address_space import (
    AddressSpaceModel,
    NodeDefinition,
    load_address_space,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
SAMPLE = REPO_ROOT / "config" / "opcua-address-space.yaml"


def test_loads_sample_file() -> None:
    model = load_address_space(SAMPLE)
    assert isinstance(model, AddressSpaceModel)
    assert model.namespace.startswith("https://")
    assert model.uns_defaults.enterprise == "uniza"
    assert any(n.id.endswith("Temperature") for n in model.iter_nodes())


def test_each_node_carries_uns_path_fields() -> None:
    model = load_address_space(SAMPLE)
    for node in model.iter_nodes():
        assert node.cell
        assert node.equipment
        assert node.measurement


def test_setpoint_nodes_have_policy() -> None:
    model = load_address_space(SAMPLE)
    setpoints = [n for n in model.iter_nodes() if n.kind == "setpoint"]
    assert setpoints, "sample must contain a setpoint to close the actuation loop"
    for n in setpoints:
        assert n.policy is not None
        assert n.policy.min < n.policy.max


def test_load_rejects_missing_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_address_space(tmp_path / "missing.yaml")


def test_load_rejects_malformed_yaml(tmp_path: Path) -> None:
    bad = tmp_path / "bad.yaml"
    bad.write_text(": : :\n")
    with pytest.raises(ValueError):
        load_address_space(bad)
```

- [ ] **Step 3: Run the test (red)**

Run:
```bash
uv run pytest services/opcua-simulator/tests/test_address_space.py -v
```
Expected: ImportError on `opcua_simulator.address_space`.

- [ ] **Step 4: Write `services/opcua-simulator/src/opcua_simulator/address_space.py`**

```python
"""Pydantic model + YAML loader for the simulator's address-space config."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError


class NodePolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    min: float
    max: float


class NodeDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    kind: Literal["measurement", "setpoint"]
    cell: str
    equipment: str
    measurement: str
    value_type: Literal["double", "int64", "bool", "string"]
    unit: str
    initial: float | int | bool | str
    semantic_type: str
    dynamics: str | None = None
    policy: NodePolicy | None = None


class EquipmentDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    nodes: list[NodeDefinition]


class UNSDefaults(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enterprise: str
    site: str
    area: str
    line: str


class AddressSpaceModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    namespace: str
    uns_defaults: UNSDefaults
    equipments: list[EquipmentDefinition] = Field(default_factory=list)

    def iter_nodes(self) -> Iterator[NodeDefinition]:
        for eq in self.equipments:
            yield from eq.nodes


def load_address_space(path: Path) -> AddressSpaceModel:
    """Load and validate the address space YAML at *path*."""
    if not path.is_file():
        raise FileNotFoundError(path)
    try:
        raw = yaml.safe_load(path.read_text())
    except yaml.YAMLError as exc:
        raise ValueError(f"malformed YAML at {path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise ValueError(f"address space at {path} must be a mapping at the top level")
    try:
        return AddressSpaceModel.model_validate(raw)
    except ValidationError as exc:
        raise ValueError(f"invalid address space at {path}: {exc}") from exc
```

- [ ] **Step 5: Run the test (green)**

Run:
```bash
uv run pytest services/opcua-simulator/tests/test_address_space.py -v
uv run mypy services/opcua-simulator/src
```
Expected: pass.

- [ ] **Step 6: Commit**

```bash
git add config/opcua-address-space.yaml \
        services/opcua-simulator/src/opcua_simulator/address_space.py \
        services/opcua-simulator/tests/test_address_space.py
git commit -m "feat(simulator): address-space model + sample bottling-line config"
```

---

### Task 17: `opcua-simulator` — seeded PRNG wrapper

**Files:**
- Create: `services/opcua-simulator/src/opcua_simulator/rng.py`
- Create: `services/opcua-simulator/tests/test_rng.py`

- [ ] **Step 1: Write the failing test `services/opcua-simulator/tests/test_rng.py`**

```python
from opcua_simulator.rng import SimulatorRNG


def test_same_seed_yields_identical_sequence() -> None:
    a = SimulatorRNG(seed=42)
    b = SimulatorRNG(seed=42)
    assert [a.gauss(0, 1) for _ in range(10)] == [b.gauss(0, 1) for _ in range(10)]
    assert [a.random() for _ in range(10)] == [b.random() for _ in range(10)]


def test_different_seeds_diverge() -> None:
    a = SimulatorRNG(seed=42)
    b = SimulatorRNG(seed=43)
    assert [a.gauss(0, 1) for _ in range(5)] != [b.gauss(0, 1) for _ in range(5)]


def test_seed_zero_is_valid_and_deterministic() -> None:
    a = SimulatorRNG(seed=0)
    b = SimulatorRNG(seed=0)
    assert a.random() == b.random()
```

- [ ] **Step 2: Run the test (red)**

Run:
```bash
uv run pytest services/opcua-simulator/tests/test_rng.py -v
```
Expected: ImportError on `opcua_simulator.rng`.

- [ ] **Step 3: Write `services/opcua-simulator/src/opcua_simulator/rng.py`**

```python
"""Deterministic PRNG wrapper for the simulator (spec §9.4).

Wraps :class:`random.Random` so the rest of the code can call ``gauss`` /
``random`` / ``uniform`` against a single seeded instance. Same seed across
runs ⇒ bit-identical OPC UA trace.
"""

from __future__ import annotations

import random


class SimulatorRNG:
    def __init__(self, seed: int) -> None:
        self._rng = random.Random(seed)

    def random(self) -> float:
        return self._rng.random()

    def uniform(self, a: float, b: float) -> float:
        return self._rng.uniform(a, b)

    def gauss(self, mu: float, sigma: float) -> float:
        return self._rng.gauss(mu, sigma)

    def randint(self, a: int, b: int) -> int:
        return self._rng.randint(a, b)
```

- [ ] **Step 4: Run the test (green)**

Run:
```bash
uv run pytest services/opcua-simulator/tests/test_rng.py -v
uv run mypy services/opcua-simulator/src
```
Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add services/opcua-simulator/src/opcua_simulator/rng.py \
        services/opcua-simulator/tests/test_rng.py
git commit -m "feat(simulator): deterministic seeded PRNG"
```

---

### Task 18: `opcua-simulator` — temperature dynamics

**Files:**
- Create: `services/opcua-simulator/src/opcua_simulator/temperature.py`
- Create: `services/opcua-simulator/tests/test_temperature.py`

Implements the mean-reverting random walk from spec §9.2.

- [ ] **Step 1: Write the failing test `services/opcua-simulator/tests/test_temperature.py`**

```python
from opcua_simulator.rng import SimulatorRNG
from opcua_simulator.temperature import TemperatureDynamics


def test_zero_noise_converges_toward_setpoint() -> None:
    rng = SimulatorRNG(seed=1)
    dyn = TemperatureDynamics(initial=10.0, alpha=0.5, sigma=0.0, rng=rng)
    for _ in range(20):
        dyn.tick(setpoint=22.0, spike_contribution=0.0)
    assert abs(dyn.value - 22.0) < 1e-6


def test_spike_contribution_lifts_value() -> None:
    rng = SimulatorRNG(seed=1)
    dyn = TemperatureDynamics(initial=22.0, alpha=0.0, sigma=0.0, rng=rng)
    dyn.tick(setpoint=22.0, spike_contribution=5.0)
    assert dyn.value == 27.0


def test_deterministic_under_same_seed() -> None:
    rng_a = SimulatorRNG(seed=42)
    rng_b = SimulatorRNG(seed=42)
    a = TemperatureDynamics(initial=22.0, alpha=0.05, sigma=0.3, rng=rng_a)
    b = TemperatureDynamics(initial=22.0, alpha=0.05, sigma=0.3, rng=rng_b)
    for _ in range(50):
        a.tick(setpoint=22.0, spike_contribution=0.0)
        b.tick(setpoint=22.0, spike_contribution=0.0)
    assert a.value == b.value


def test_setpoint_change_is_tracked_within_50_ticks() -> None:
    rng = SimulatorRNG(seed=7)
    dyn = TemperatureDynamics(initial=22.0, alpha=0.1, sigma=0.0, rng=rng)
    for _ in range(50):
        dyn.tick(setpoint=18.0, spike_contribution=0.0)
    assert abs(dyn.value - 18.0) < 0.1
```

- [ ] **Step 2: Run the test (red)**

Run:
```bash
uv run pytest services/opcua-simulator/tests/test_temperature.py -v
```
Expected: ImportError on `opcua_simulator.temperature`.

- [ ] **Step 3: Write `services/opcua-simulator/src/opcua_simulator/temperature.py`**

```python
"""Mean-reverting temperature dynamics (spec §9.2)."""

from __future__ import annotations

from dataclasses import dataclass

from opcua_simulator.rng import SimulatorRNG


@dataclass
class TemperatureDynamics:
    """Per-tick temperature update:

        T_t = T_{t-1} + alpha * (setpoint - T_{t-1}) + Gaussian(0, sigma) + spike
    """

    initial: float
    alpha: float
    sigma: float
    rng: SimulatorRNG

    def __post_init__(self) -> None:
        self.value: float = float(self.initial)

    def tick(self, *, setpoint: float, spike_contribution: float) -> float:
        noise = self.rng.gauss(0.0, self.sigma) if self.sigma > 0.0 else 0.0
        self.value = self.value + self.alpha * (setpoint - self.value) + noise + spike_contribution
        return self.value
```

- [ ] **Step 4: Run the test (green)**

Run:
```bash
uv run pytest services/opcua-simulator/tests/test_temperature.py -v
uv run mypy services/opcua-simulator/src
```
Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add services/opcua-simulator/src/opcua_simulator/temperature.py \
        services/opcua-simulator/tests/test_temperature.py
git commit -m "feat(simulator): mean-reverting temperature dynamics"
```

---

### Task 19: `opcua-simulator` — motor state machine + RPM

**Files:**
- Create: `services/opcua-simulator/src/opcua_simulator/motor.py`
- Create: `services/opcua-simulator/tests/test_motor.py`

State machine from spec §9.2. State codes match the address-space config:
`0=stopped`, `1=starting`, `2=running`, `3=fault`.

- [ ] **Step 1: Write the failing test `services/opcua-simulator/tests/test_motor.py`**

```python
from opcua_simulator.motor import (
    MOTOR_FAULT,
    MOTOR_RUNNING,
    MOTOR_STARTING,
    MOTOR_STOPPED,
    Motor,
)
from opcua_simulator.rng import SimulatorRNG


def _zero_fault_rng() -> SimulatorRNG:
    return SimulatorRNG(seed=0)


def test_starts_stopped() -> None:
    m = Motor(rng=_zero_fault_rng(), tick_ms=100, fault_probability=0.0)
    assert m.state == MOTOR_STOPPED
    assert m.rpm == 0.0


def test_transitions_stopped_to_starting_after_5_seconds() -> None:
    m = Motor(rng=_zero_fault_rng(), tick_ms=100, fault_probability=0.0)
    for _ in range(49):
        m.tick()
        assert m.state == MOTOR_STOPPED
    m.tick()  # 50th tick = 5 seconds at 100ms
    assert m.state == MOTOR_STARTING


def test_transitions_starting_to_running_after_3_seconds() -> None:
    m = Motor(rng=_zero_fault_rng(), tick_ms=100, fault_probability=0.0)
    for _ in range(50):
        m.tick()
    # now in MOTOR_STARTING
    for _ in range(30):
        m.tick()
    assert m.state == MOTOR_RUNNING
    assert 1400 <= m.rpm <= 1600  # noise ±50 around 1500


def test_starting_ramp_increases_rpm_monotonically() -> None:
    m = Motor(rng=_zero_fault_rng(), tick_ms=100, fault_probability=0.0)
    for _ in range(50):
        m.tick()
    # in starting state — RPM should ramp from 0 toward 1500
    samples = []
    for _ in range(30):
        m.tick()
        samples.append(m.rpm)
    assert samples[0] < samples[-1]


def test_fault_probability_one_forces_fault_when_running() -> None:
    m = Motor(rng=_zero_fault_rng(), tick_ms=100, fault_probability=1.0)
    for _ in range(80):
        m.tick()
    # In running state, every tick has p=1 of fault
    m.tick()
    assert m.state == MOTOR_FAULT
    assert m.rpm == 0.0


def test_fault_recovers_to_stopped_after_10_seconds() -> None:
    m = Motor(rng=_zero_fault_rng(), tick_ms=100, fault_probability=1.0)
    for _ in range(81):
        m.tick()
    assert m.state == MOTOR_FAULT
    for _ in range(100):
        m.tick()
    assert m.state == MOTOR_STOPPED
```

- [ ] **Step 2: Run the test (red)**

Run:
```bash
uv run pytest services/opcua-simulator/tests/test_motor.py -v
```
Expected: ImportError on `opcua_simulator.motor`.

- [ ] **Step 3: Write `services/opcua-simulator/src/opcua_simulator/motor.py`**

```python
"""Motor state machine + RPM dynamics (spec §9.2)."""

from __future__ import annotations

from dataclasses import dataclass

from opcua_simulator.rng import SimulatorRNG

MOTOR_STOPPED = 0
MOTOR_STARTING = 1
MOTOR_RUNNING = 2
MOTOR_FAULT = 3

_STOPPED_TO_STARTING_MS = 5_000
_STARTING_TO_RUNNING_MS = 3_000
_FAULT_TO_STOPPED_MS = 10_000

_TARGET_RPM = 1500.0
_RPM_NOISE = 50.0


@dataclass
class Motor:
    rng: SimulatorRNG
    tick_ms: int
    fault_probability: float

    def __post_init__(self) -> None:
        self.state: int = MOTOR_STOPPED
        self.rpm: float = 0.0
        self._time_in_state_ms: int = 0

    def tick(self) -> None:
        self._time_in_state_ms += self.tick_ms
        if self.state == MOTOR_STOPPED:
            if self._time_in_state_ms >= _STOPPED_TO_STARTING_MS:
                self._enter(MOTOR_STARTING)
        elif self.state == MOTOR_STARTING:
            ramp = min(1.0, self._time_in_state_ms / _STARTING_TO_RUNNING_MS)
            self.rpm = ramp * _TARGET_RPM
            if self._time_in_state_ms >= _STARTING_TO_RUNNING_MS:
                self._enter(MOTOR_RUNNING)
        elif self.state == MOTOR_RUNNING:
            self.rpm = _TARGET_RPM + self.rng.gauss(0.0, _RPM_NOISE)
            if self.rng.random() < self.fault_probability:
                self._enter(MOTOR_FAULT)
        elif self.state == MOTOR_FAULT:
            if self._time_in_state_ms >= _FAULT_TO_STOPPED_MS:
                self._enter(MOTOR_STOPPED)

    def _enter(self, state: int) -> None:
        self.state = state
        self._time_in_state_ms = 0
        if state in (MOTOR_STOPPED, MOTOR_FAULT):
            self.rpm = 0.0
        elif state == MOTOR_STARTING:
            self.rpm = 0.0
```

- [ ] **Step 4: Run the test (green)**

Run:
```bash
uv run pytest services/opcua-simulator/tests/test_motor.py -v
uv run mypy services/opcua-simulator/src
```
Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add services/opcua-simulator/src/opcua_simulator/motor.py \
        services/opcua-simulator/tests/test_motor.py
git commit -m "feat(simulator): motor state machine and RPM dynamics"
```

---

### Task 20: `opcua-simulator` — throughput dynamics

**Files:**
- Create: `services/opcua-simulator/src/opcua_simulator/throughput.py`
- Create: `services/opcua-simulator/tests/test_throughput.py`

- [ ] **Step 1: Write the failing test `services/opcua-simulator/tests/test_throughput.py`**

```python
from opcua_simulator.motor import MOTOR_FAULT, MOTOR_RUNNING, MOTOR_STARTING, MOTOR_STOPPED
from opcua_simulator.rng import SimulatorRNG
from opcua_simulator.throughput import Throughput


def test_stopped_throughput_is_zero() -> None:
    t = Throughput(rng=SimulatorRNG(seed=0))
    assert t.compute(motor_state=MOTOR_STOPPED, motor_rpm=0.0) == 0.0


def test_fault_throughput_is_zero() -> None:
    t = Throughput(rng=SimulatorRNG(seed=0))
    assert t.compute(motor_state=MOTOR_FAULT, motor_rpm=0.0) == 0.0


def test_running_throughput_scales_with_rpm() -> None:
    t = Throughput(rng=SimulatorRNG(seed=0))
    low = t.compute(motor_state=MOTOR_RUNNING, motor_rpm=750.0)
    high = t.compute(motor_state=MOTOR_RUNNING, motor_rpm=1500.0)
    assert high > low


def test_running_throughput_at_1500_rpm_is_near_target() -> None:
    samples = []
    t = Throughput(rng=SimulatorRNG(seed=0))
    for _ in range(200):
        samples.append(t.compute(motor_state=MOTOR_RUNNING, motor_rpm=1500.0))
    mean = sum(samples) / len(samples)
    assert 0.8 < mean < 1.0


def test_starting_ramps_with_rpm() -> None:
    t = Throughput(rng=SimulatorRNG(seed=0))
    early = t.compute(motor_state=MOTOR_STARTING, motor_rpm=300.0)
    late = t.compute(motor_state=MOTOR_STARTING, motor_rpm=1200.0)
    assert late > early
```

- [ ] **Step 2: Run the test (red)**

Run:
```bash
uv run pytest services/opcua-simulator/tests/test_throughput.py -v
```
Expected: ImportError on `opcua_simulator.throughput`.

- [ ] **Step 3: Write `services/opcua-simulator/src/opcua_simulator/throughput.py`**

```python
"""Throughput dynamics (spec §9.2)."""

from __future__ import annotations

from dataclasses import dataclass

from opcua_simulator.motor import MOTOR_FAULT, MOTOR_RUNNING, MOTOR_STARTING
from opcua_simulator.rng import SimulatorRNG

_RPM_TO_BPS = 0.0006  # bottles/s per rpm at running


@dataclass
class Throughput:
    rng: SimulatorRNG

    def compute(self, *, motor_state: int, motor_rpm: float) -> float:
        if motor_state == MOTOR_RUNNING:
            return max(0.0, _RPM_TO_BPS * motor_rpm + self.rng.gauss(0.0, 0.05))
        if motor_state == MOTOR_STARTING:
            return max(0.0, _RPM_TO_BPS * motor_rpm)
        # stopped or fault
        return 0.0
```

- [ ] **Step 4: Run the test (green)**

Run:
```bash
uv run pytest services/opcua-simulator/tests/test_throughput.py -v
uv run mypy services/opcua-simulator/src
```
Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add services/opcua-simulator/src/opcua_simulator/throughput.py \
        services/opcua-simulator/tests/test_throughput.py
git commit -m "feat(simulator): throughput dynamics tied to motor state"
```

---

### Task 21: `opcua-simulator` — setpoint write handling

**Files:**
- Create: `services/opcua-simulator/src/opcua_simulator/setpoint.py`
- Create: `services/opcua-simulator/tests/test_setpoint.py`

Records writes (writer session ID, timestamp, value) so e2e tests can confirm a write reached the device. The simulator accepts writes unconditionally (validation lives in the EirVah path).

- [ ] **Step 1: Write the failing test `services/opcua-simulator/tests/test_setpoint.py`**

```python
from datetime import datetime, timezone

from opcua_simulator.setpoint import Setpoint, SetpointWrite


def test_initial_value_is_default() -> None:
    sp = Setpoint(initial=22.0)
    assert sp.value == 22.0
    assert sp.write_history() == []


def test_write_takes_effect_immediately() -> None:
    sp = Setpoint(initial=22.0)
    sp.write(
        value=18.0,
        writer_session="opcua-session-1",
        at=datetime(2026, 5, 16, 13, 45, 22, tzinfo=timezone.utc),
    )
    assert sp.value == 18.0


def test_write_history_records_each_write() -> None:
    sp = Setpoint(initial=22.0)
    now = datetime(2026, 5, 16, 13, 45, 22, tzinfo=timezone.utc)
    sp.write(value=18.0, writer_session="s1", at=now)
    sp.write(value=20.0, writer_session="s2", at=now)
    history = sp.write_history()
    assert len(history) == 2
    assert history[0] == SetpointWrite(value=18.0, writer_session="s1", at=now)
    assert history[1].value == 20.0


def test_write_count_increments() -> None:
    sp = Setpoint(initial=22.0)
    now = datetime(2026, 5, 16, 13, 45, 22, tzinfo=timezone.utc)
    sp.write(value=18.0, writer_session="s1", at=now)
    sp.write(value=20.0, writer_session="s1", at=now)
    assert sp.write_count_by_writer() == {"s1": 2}
```

- [ ] **Step 2: Run the test (red)**

Run:
```bash
uv run pytest services/opcua-simulator/tests/test_setpoint.py -v
```
Expected: ImportError on `opcua_simulator.setpoint`.

- [ ] **Step 3: Write `services/opcua-simulator/src/opcua_simulator/setpoint.py`**

```python
"""Setpoint state + write audit trail (spec §9.2, §9.5)."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime


@dataclass(frozen=True)
class SetpointWrite:
    value: float
    writer_session: str
    at: datetime


@dataclass
class Setpoint:
    initial: float

    def __post_init__(self) -> None:
        self.value: float = float(self.initial)
        self._writes: list[SetpointWrite] = []

    def write(self, *, value: float, writer_session: str, at: datetime) -> None:
        self.value = float(value)
        self._writes.append(SetpointWrite(value=float(value), writer_session=writer_session, at=at))

    def write_history(self) -> list[SetpointWrite]:
        return list(self._writes)

    def write_count_by_writer(self) -> dict[str, int]:
        return dict(Counter(w.writer_session for w in self._writes))
```

- [ ] **Step 4: Run the test (green)**

Run:
```bash
uv run pytest services/opcua-simulator/tests/test_setpoint.py -v
uv run mypy services/opcua-simulator/src
```
Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add services/opcua-simulator/src/opcua_simulator/setpoint.py \
        services/opcua-simulator/tests/test_setpoint.py
git commit -m "feat(simulator): setpoint state with per-writer audit trail"
```

---

### Task 22: `opcua-simulator` — hot-spike trigger

**Files:**
- Create: `services/opcua-simulator/src/opcua_simulator/hot_spike.py`
- Create: `services/opcua-simulator/tests/test_hot_spike.py`

Two trigger sources: stochastic (per-tick probability) and on-demand (OPC UA method call). The hot spike adds +5°C on the trigger tick, decaying at 0.9^n on subsequent ticks.

- [ ] **Step 1: Write the failing test `services/opcua-simulator/tests/test_hot_spike.py`**

```python
from opcua_simulator.hot_spike import HotSpike, SPIKE_AMPLITUDE
from opcua_simulator.rng import SimulatorRNG


def test_no_spike_when_probability_zero_and_no_external_trigger() -> None:
    hs = HotSpike(rng=SimulatorRNG(seed=0), stochastic_probability=0.0)
    contributions = [hs.tick() for _ in range(20)]
    assert all(c == 0.0 for c in contributions)


def test_method_trigger_emits_amplitude_on_next_tick() -> None:
    hs = HotSpike(rng=SimulatorRNG(seed=0), stochastic_probability=0.0)
    hs.trigger_via_method()
    assert hs.tick() == SPIKE_AMPLITUDE


def test_spike_decays_at_0_9_per_tick() -> None:
    hs = HotSpike(rng=SimulatorRNG(seed=0), stochastic_probability=0.0)
    hs.trigger_via_method()
    first = hs.tick()
    second = hs.tick()
    third = hs.tick()
    assert first == SPIKE_AMPLITUDE
    assert abs(second - SPIKE_AMPLITUDE * 0.9) < 1e-9
    assert abs(third - SPIKE_AMPLITUDE * 0.9 * 0.9) < 1e-9


def test_stochastic_probability_one_always_triggers() -> None:
    hs = HotSpike(rng=SimulatorRNG(seed=0), stochastic_probability=1.0)
    assert hs.tick() == SPIKE_AMPLITUDE


def test_trigger_counts_by_source() -> None:
    hs = HotSpike(rng=SimulatorRNG(seed=0), stochastic_probability=1.0)
    hs.tick()  # stochastic
    hs.trigger_via_method()
    hs.tick()  # method
    counts = hs.trigger_counts()
    assert counts["stochastic"] == 1
    assert counts["method"] == 1
```

- [ ] **Step 2: Run the test (red)**

Run:
```bash
uv run pytest services/opcua-simulator/tests/test_hot_spike.py -v
```
Expected: ImportError on `opcua_simulator.hot_spike`.

- [ ] **Step 3: Write `services/opcua-simulator/src/opcua_simulator/hot_spike.py`**

```python
"""Hot-spike trigger: stochastic + OPC UA method (spec §9.2)."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass

from opcua_simulator.rng import SimulatorRNG

SPIKE_AMPLITUDE = 5.0
SPIKE_DECAY = 0.9


@dataclass
class HotSpike:
    rng: SimulatorRNG
    stochastic_probability: float

    def __post_init__(self) -> None:
        self._spike_contribution: float = 0.0
        self._method_triggered: bool = False
        self._triggers: Counter[str] = Counter()

    def trigger_via_method(self) -> None:
        """Fire a hot spike on the next ``tick()`` call."""
        self._method_triggered = True

    def tick(self) -> float:
        """Advance one tick. Returns the additive temperature contribution."""
        # Decay any in-flight spike
        self._spike_contribution *= SPIKE_DECAY

        triggered_kind: str | None = None
        if self._method_triggered:
            triggered_kind = "method"
            self._method_triggered = False
        elif self.rng.random() < self.stochastic_probability:
            triggered_kind = "stochastic"

        if triggered_kind is not None:
            self._spike_contribution = SPIKE_AMPLITUDE
            self._triggers[triggered_kind] += 1

        return self._spike_contribution

    def trigger_counts(self) -> dict[str, int]:
        return dict(self._triggers)
```

- [ ] **Step 4: Run the test (green)**

Run:
```bash
uv run pytest services/opcua-simulator/tests/test_hot_spike.py -v
uv run mypy services/opcua-simulator/src
```
Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add services/opcua-simulator/src/opcua_simulator/hot_spike.py \
        services/opcua-simulator/tests/test_hot_spike.py
git commit -m "feat(simulator): hot-spike trigger (stochastic and method-driven)"
```

---

### Task 23: `opcua-simulator` — quality code emission

**Files:**
- Create: `services/opcua-simulator/src/opcua_simulator/quality.py`
- Create: `services/opcua-simulator/tests/test_quality.py`

Per-node config can request a percentage of `Bad` or `Uncertain` samples (spec §9.3).

- [ ] **Step 1: Write the failing test `services/opcua-simulator/tests/test_quality.py`**

```python
from opcua_simulator.quality import QualityEmitter
from opcua_simulator.rng import SimulatorRNG


def test_default_is_always_good() -> None:
    q = QualityEmitter(rng=SimulatorRNG(seed=0), bad_quality_pct=0.0, uncertain_quality_pct=0.0)
    assert all(q.next() == "good" for _ in range(100))


def test_full_bad_pct_always_returns_bad() -> None:
    q = QualityEmitter(rng=SimulatorRNG(seed=0), bad_quality_pct=1.0, uncertain_quality_pct=0.0)
    assert all(q.next() == "bad" for _ in range(50))


def test_partial_bad_pct_is_approximately_correct() -> None:
    q = QualityEmitter(rng=SimulatorRNG(seed=42), bad_quality_pct=0.1, uncertain_quality_pct=0.0)
    samples = [q.next() for _ in range(10_000)]
    bad_ratio = samples.count("bad") / len(samples)
    assert 0.08 < bad_ratio < 0.12


def test_bad_takes_precedence_over_uncertain() -> None:
    # With bad_pct + uncertain_pct > 1, bad wins on collision.
    q = QualityEmitter(rng=SimulatorRNG(seed=0), bad_quality_pct=1.0, uncertain_quality_pct=1.0)
    assert q.next() == "bad"


def test_counters_track_emissions() -> None:
    q = QualityEmitter(rng=SimulatorRNG(seed=0), bad_quality_pct=1.0, uncertain_quality_pct=0.0)
    for _ in range(5):
        q.next()
    counts = q.emission_counts()
    assert counts["bad"] == 5
    assert counts["good"] == 0
```

- [ ] **Step 2: Run the test (red)**

Run:
```bash
uv run pytest services/opcua-simulator/tests/test_quality.py -v
```
Expected: ImportError on `opcua_simulator.quality`.

- [ ] **Step 3: Write `services/opcua-simulator/src/opcua_simulator/quality.py`**

```python
"""Configurable per-node quality emission (spec §9.3)."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Literal

from opcua_simulator.rng import SimulatorRNG

Quality = Literal["good", "uncertain", "bad"]


@dataclass
class QualityEmitter:
    rng: SimulatorRNG
    bad_quality_pct: float
    uncertain_quality_pct: float

    def __post_init__(self) -> None:
        self._counts: Counter[str] = Counter()

    def next(self) -> Quality:
        r = self.rng.random()
        # Bad wins ties / overlaps with uncertain.
        if r < self.bad_quality_pct:
            self._counts["bad"] += 1
            return "bad"
        if r < self.bad_quality_pct + self.uncertain_quality_pct:
            self._counts["uncertain"] += 1
            return "uncertain"
        self._counts["good"] += 1
        return "good"

    def emission_counts(self) -> dict[str, int]:
        return dict(self._counts)
```

- [ ] **Step 4: Run the test (green)**

Run:
```bash
uv run pytest services/opcua-simulator/tests/test_quality.py -v
uv run mypy services/opcua-simulator/src
```
Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add services/opcua-simulator/src/opcua_simulator/quality.py \
        services/opcua-simulator/tests/test_quality.py
git commit -m "feat(simulator): per-node configurable quality emission"
```

---

### Task 24: `opcua-simulator` — Prometheus state metrics

The simulator's `/metrics` surface (spec §9.5). Every gauge and counter listed there gets a factory call here.

**Files:**
- Create: `services/opcua-simulator/src/opcua_simulator/metrics.py`
- Create: `services/opcua-simulator/tests/test_metrics.py`

- [ ] **Step 1: Write the failing test `services/opcua-simulator/tests/test_metrics.py`**

```python
from __future__ import annotations

from prometheus_client import CollectorRegistry

from opcua_simulator.metrics import SimulatorMetrics


def _labels() -> dict[str, str]:
    return {
        "enterprise": "uniza",
        "site": "zilina",
        "area": "factory1",
        "line": "line_a",
        "cell": "bottler",
        "equipment": "temperature_sensor_01",
    }


def test_gauges_register_with_eirvah_prefix() -> None:
    reg = CollectorRegistry()
    m = SimulatorMetrics(registry=reg)
    m.set_temperature(_labels() | {"equipment": "temperature_sensor_01"}, 23.4)
    m.set_setpoint(_labels() | {"equipment": "setpoint_unit"}, 22.0)
    m.set_motor_state(_labels() | {"equipment": "motor_01"}, 2)
    m.set_motor_rpm(_labels() | {"equipment": "motor_01"}, 1500.0)
    m.set_throughput(_labels() | {"equipment": "throughput_meter_01"}, 0.9)

    assert reg.get_sample_value(
        "eirvah_simulator_temperature_celsius",
        _labels() | {"equipment": "temperature_sensor_01"},
    ) == 23.4
    assert reg.get_sample_value(
        "eirvah_simulator_setpoint_celsius",
        _labels() | {"equipment": "setpoint_unit"},
    ) == 22.0
    assert reg.get_sample_value(
        "eirvah_simulator_motor_state",
        _labels() | {"equipment": "motor_01"},
    ) == 2


def test_quality_counter_increments() -> None:
    reg = CollectorRegistry()
    m = SimulatorMetrics(registry=reg)
    labels = _labels() | {"equipment": "temperature_sensor_01"}
    m.inc_quality(labels=labels, quality="good")
    m.inc_quality(labels=labels, quality="good")
    m.inc_quality(labels=labels, quality="bad")
    assert reg.get_sample_value(
        "eirvah_simulator_quality_count_total",
        labels | {"quality": "good"},
    ) == 2
    assert reg.get_sample_value(
        "eirvah_simulator_quality_count_total",
        labels | {"quality": "bad"},
    ) == 1


def test_setpoint_writes_counter() -> None:
    reg = CollectorRegistry()
    m = SimulatorMetrics(registry=reg)
    m.inc_setpoint_write(writer="opcua-session-1")
    m.inc_setpoint_write(writer="opcua-session-1")
    assert reg.get_sample_value(
        "eirvah_simulator_setpoint_writes_total",
        {"writer": "opcua-session-1"},
    ) == 2


def test_hot_spike_counter_by_trigger() -> None:
    reg = CollectorRegistry()
    m = SimulatorMetrics(registry=reg)
    m.inc_hot_spike(trigger="method")
    m.inc_hot_spike(trigger="stochastic")
    m.inc_hot_spike(trigger="stochastic")
    assert reg.get_sample_value(
        "eirvah_simulator_hot_spikes_total",
        {"trigger": "method"},
    ) == 1
    assert reg.get_sample_value(
        "eirvah_simulator_hot_spikes_total",
        {"trigger": "stochastic"},
    ) == 2
```

- [ ] **Step 2: Run the test (red)**

Run:
```bash
uv run pytest services/opcua-simulator/tests/test_metrics.py -v
```
Expected: ImportError on `opcua_simulator.metrics`.

- [ ] **Step 3: Write `services/opcua-simulator/src/opcua_simulator/metrics.py`**

```python
"""Prometheus surface for the simulator (spec §9.5).

Labels carry the ISA-95 hierarchy so Grafana panels can filter or aggregate
by enterprise/site/area/line/cell/equipment without joining a separate dim.
"""

from __future__ import annotations

from prometheus_client.registry import REGISTRY, CollectorRegistry

from eirvah_observability.metrics import make_counter, make_gauge

_ISA95_LABELS = (
    "enterprise",
    "site",
    "area",
    "line",
    "cell",
    "equipment",
)


class SimulatorMetrics:
    """Facade for every Prometheus metric the simulator emits."""

    def __init__(self, registry: CollectorRegistry = REGISTRY) -> None:
        self._temperature = make_gauge(
            "simulator_temperature_celsius",
            "Current temperature reading from the simulator (°C).",
            labelnames=_ISA95_LABELS,
            registry=registry,
        )
        self._setpoint = make_gauge(
            "simulator_setpoint_celsius",
            "Current setpoint value (°C).",
            labelnames=_ISA95_LABELS,
            registry=registry,
        )
        self._throughput = make_gauge(
            "simulator_throughput_bottles_per_second",
            "Current throughput (bottles/s).",
            labelnames=_ISA95_LABELS,
            registry=registry,
        )
        self._motor_state = make_gauge(
            "simulator_motor_state",
            "Motor state: 0=stopped 1=starting 2=running 3=fault.",
            labelnames=_ISA95_LABELS,
            registry=registry,
        )
        self._motor_rpm = make_gauge(
            "simulator_motor_rpm",
            "Motor RPM.",
            labelnames=_ISA95_LABELS,
            registry=registry,
        )
        self._quality_count = make_counter(
            "simulator_quality_count",
            "Samples emitted per quality bucket.",
            labelnames=(*_ISA95_LABELS, "quality"),
            registry=registry,
        )
        self._setpoint_writes = make_counter(
            "simulator_setpoint_writes",
            "Setpoint writes received from the EirVah pipeline.",
            labelnames=("writer",),
            registry=registry,
        )
        self._hot_spikes = make_counter(
            "simulator_hot_spikes",
            "Hot-spike triggers fired, by source.",
            labelnames=("trigger",),
            registry=registry,
        )

    def set_temperature(self, labels: dict[str, str], value: float) -> None:
        self._temperature.labels(**labels).set(value)

    def set_setpoint(self, labels: dict[str, str], value: float) -> None:
        self._setpoint.labels(**labels).set(value)

    def set_throughput(self, labels: dict[str, str], value: float) -> None:
        self._throughput.labels(**labels).set(value)

    def set_motor_state(self, labels: dict[str, str], state: int) -> None:
        self._motor_state.labels(**labels).set(state)

    def set_motor_rpm(self, labels: dict[str, str], rpm: float) -> None:
        self._motor_rpm.labels(**labels).set(rpm)

    def inc_quality(self, *, labels: dict[str, str], quality: str) -> None:
        self._quality_count.labels(**labels, quality=quality).inc()

    def inc_setpoint_write(self, *, writer: str) -> None:
        self._setpoint_writes.labels(writer=writer).inc()

    def inc_hot_spike(self, *, trigger: str) -> None:
        self._hot_spikes.labels(trigger=trigger).inc()
```

- [ ] **Step 4: Run the test (green)**

Run:
```bash
uv run pytest services/opcua-simulator/tests/test_metrics.py -v
uv run mypy services/opcua-simulator/src
```
Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add services/opcua-simulator/src/opcua_simulator/metrics.py \
        services/opcua-simulator/tests/test_metrics.py
git commit -m "feat(simulator): Prometheus state metrics for device-truth view"
```

---

### Task 25: `opcua-simulator` — OPC UA server + tick loop wiring

This task brings everything together: starts the asyncua server, binds the address space, wires the dynamics into a tick loop, starts the ASGI server with `/healthz` `/readyz` `/metrics`, and registers a `TriggerHotSpike` OPC UA method.

**Files:**
- Modify: `services/opcua-simulator/src/opcua_simulator/__main__.py`
- Create: `services/opcua-simulator/src/opcua_simulator/server.py`
- Create: `services/opcua-simulator/tests/test_server_smoke.py`

- [ ] **Step 1: Write `services/opcua-simulator/src/opcua_simulator/server.py`**

```python
"""OPC UA server + tick loop wiring (spec §9)."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import structlog
import uvicorn
from asyncua import Server, ua

from eirvah_observability.health import HealthApp
from eirvah_observability.logging import bind_correlation_id, configure_logging
from opcua_simulator.address_space import AddressSpaceModel, NodeDefinition, load_address_space
from opcua_simulator.config import SimulatorSettings
from opcua_simulator.hot_spike import HotSpike
from opcua_simulator.metrics import SimulatorMetrics
from opcua_simulator.motor import MOTOR_FAULT, MOTOR_RUNNING, MOTOR_STARTING, Motor
from opcua_simulator.quality import QualityEmitter
from opcua_simulator.rng import SimulatorRNG
from opcua_simulator.setpoint import Setpoint
from opcua_simulator.temperature import TemperatureDynamics
from opcua_simulator.throughput import Throughput

if TYPE_CHECKING:
    from asyncua.common.node import Node


_log = structlog.get_logger("opcua-simulator")


class SimulatorRuntime:
    def __init__(self, settings: SimulatorSettings) -> None:
        self.settings = settings
        self.rng = SimulatorRNG(seed=settings.seed)
        self.metrics = SimulatorMetrics()
        self._address_space: AddressSpaceModel | None = None
        self._setpoint: Setpoint | None = None
        self._temperature: TemperatureDynamics | None = None
        self._motor: Motor | None = None
        self._throughput: Throughput | None = None
        self._hot_spike: HotSpike | None = None
        self._quality_per_node: dict[str, QualityEmitter] = {}
        self._nodes_by_def_id: dict[str, Node] = {}
        self._server: Server | None = None
        self._ready: bool = False

    def is_ready(self) -> bool:
        return self._ready

    async def start(self) -> None:
        self._address_space = load_address_space(self.settings.address_space_path)
        self._build_dynamics()

        self._server = Server()
        await self._server.init()
        self._server.set_endpoint(self.settings.endpoint)
        self._server.set_server_name("EirVah Bottling Line Simulator")

        idx = await self._server.register_namespace(self._address_space.namespace)
        await self._populate_address_space(idx)

        await self._server.start()
        self._ready = True
        _log.info(
            "opcua_server_started",
            endpoint=self.settings.endpoint,
            tick_rate_ms=self.settings.tick_rate_ms,
            seed=self.settings.seed,
        )

    async def stop(self) -> None:
        self._ready = False
        if self._server is not None:
            await self._server.stop()

    def _build_dynamics(self) -> None:
        assert self._address_space is not None
        # Find the setpoint node (spec §9 says there is exactly one writable setpoint).
        setpoint_def = next(
            (n for n in self._address_space.iter_nodes() if n.kind == "setpoint"),
            None,
        )
        if setpoint_def is None:
            raise ValueError("address space must contain a setpoint node")
        initial_setpoint = float(setpoint_def.initial)  # type: ignore[arg-type]
        self._setpoint = Setpoint(initial=initial_setpoint)

        # Find the temperature measurement (dynamics="temperature").
        temp_def = next(
            (n for n in self._address_space.iter_nodes() if n.dynamics == "temperature"),
            None,
        )
        if temp_def is None:
            raise ValueError("address space must contain a temperature node")
        self._temperature = TemperatureDynamics(
            initial=float(temp_def.initial),  # type: ignore[arg-type]
            alpha=0.05,
            sigma=0.3,
            rng=self.rng,
        )

        self._motor = Motor(
            rng=self.rng,
            tick_ms=self.settings.tick_rate_ms,
            fault_probability=self.settings.motor_fault_probability,
        )
        self._throughput = Throughput(rng=self.rng)
        self._hot_spike = HotSpike(
            rng=self.rng,
            stochastic_probability=self.settings.hot_spike_probability,
        )

        for node_def in self._address_space.iter_nodes():
            self._quality_per_node[node_def.id] = QualityEmitter(
                rng=self.rng,
                bad_quality_pct=0.0,
                uncertain_quality_pct=0.0,
            )

    async def _populate_address_space(self, ns_idx: int) -> None:
        assert self._server is not None and self._address_space is not None
        objects = self._server.nodes.objects
        for eq in self._address_space.equipments:
            eq_folder = await objects.add_folder(ns_idx, eq.name)
            for node_def in eq.nodes:
                writable = node_def.kind == "setpoint"
                ua_node = await eq_folder.add_variable(
                    ns_idx,
                    node_def.id.split(".")[-1],
                    self._initial_ua_value(node_def),
                    varianttype=_VALUE_TYPE_TO_VARIANT[node_def.value_type],
                )
                if writable:
                    await ua_node.set_writable(True)
                self._nodes_by_def_id[node_def.id] = ua_node

            # Register the OPC UA method that triggers a hot spike on demand.
            await eq_folder.add_method(
                ns_idx,
                "TriggerHotSpike",
                self._trigger_hot_spike_method,
                [],
                [],
            )

    async def _trigger_hot_spike_method(self, _parent: Any) -> list[Any]:
        assert self._hot_spike is not None
        self._hot_spike.trigger_via_method()
        _log.info("hot_spike_method_invoked")
        return []

    async def run_tick_loop(self) -> None:
        period_s = self.settings.tick_rate_ms / 1000.0
        while self._ready:
            await self._tick()
            await asyncio.sleep(period_s)

    async def _tick(self) -> None:
        assert self._setpoint is not None
        assert self._temperature is not None
        assert self._motor is not None
        assert self._throughput is not None
        assert self._hot_spike is not None
        assert self._address_space is not None

        # First: pick up any setpoint writes that arrived since last tick.
        await self._reconcile_setpoint()

        spike = self._hot_spike.tick()
        previous_trigger_kinds = self._hot_spike.trigger_counts()

        self._motor.tick()
        temp = self._temperature.tick(setpoint=self._setpoint.value, spike_contribution=spike)
        tput = self._throughput.compute(
            motor_state=self._motor.state, motor_rpm=self._motor.rpm
        )

        # Write back to OPC UA nodes and update Prometheus state.
        defaults = self._address_space.uns_defaults
        for node_def in self._address_space.iter_nodes():
            labels = {
                "enterprise": defaults.enterprise,
                "site": defaults.site,
                "area": defaults.area,
                "line": defaults.line,
                "cell": node_def.cell,
                "equipment": node_def.equipment,
            }
            value = self._value_for_node(
                node_def, temp=temp, tput=tput, motor=self._motor, setpoint=self._setpoint
            )
            await self._nodes_by_def_id[node_def.id].write_value(value)
            self._update_state_metric(node_def, labels=labels, value=value)
            quality = self._quality_per_node[node_def.id].next()
            self.metrics.inc_quality(labels=labels, quality=quality)

        for kind, count in self._hot_spike.trigger_counts().items():
            delta = count - previous_trigger_kinds.get(kind, 0)
            for _ in range(delta):
                self.metrics.inc_hot_spike(trigger=kind)

    async def _reconcile_setpoint(self) -> None:
        assert self._address_space is not None and self._setpoint is not None
        setpoint_def = next(
            n for n in self._address_space.iter_nodes() if n.kind == "setpoint"
        )
        ua_value = await self._nodes_by_def_id[setpoint_def.id].read_value()
        if float(ua_value) != self._setpoint.value:
            now = datetime.now(timezone.utc)
            self._setpoint.write(
                value=float(ua_value), writer_session="opcua-client", at=now
            )
            self.metrics.inc_setpoint_write(writer="opcua-client")
            _log.info("setpoint_write_observed", new_value=float(ua_value))

    def _value_for_node(
        self,
        node_def: NodeDefinition,
        *,
        temp: float,
        tput: float,
        motor: Motor,
        setpoint: Setpoint,
    ) -> Any:
        match node_def.dynamics:
            case "temperature":
                return float(temp)
            case "throughput":
                return float(tput)
            case "motor_state":
                return int(motor.state)
            case "motor_rpm":
                return float(motor.rpm)
            case None if node_def.kind == "setpoint":
                return float(setpoint.value)
            case _:
                return node_def.initial

    def _update_state_metric(
        self,
        node_def: NodeDefinition,
        *,
        labels: dict[str, str],
        value: Any,
    ) -> None:
        match node_def.dynamics:
            case "temperature":
                self.metrics.set_temperature(labels, float(value))
            case "throughput":
                self.metrics.set_throughput(labels, float(value))
            case "motor_state":
                self.metrics.set_motor_state(labels, int(value))
            case "motor_rpm":
                self.metrics.set_motor_rpm(labels, float(value))
            case None if node_def.kind == "setpoint":
                self.metrics.set_setpoint(labels, float(value))

    def _initial_ua_value(self, node_def: NodeDefinition) -> Any:
        return node_def.initial


_VALUE_TYPE_TO_VARIANT = {
    "double": ua.VariantType.Double,
    "int64": ua.VariantType.Int64,
    "bool": ua.VariantType.Boolean,
    "string": ua.VariantType.String,
}


async def run(settings: SimulatorSettings) -> None:
    configure_logging(level=settings.log_level)
    bind_correlation_id("system")
    runtime = SimulatorRuntime(settings)
    health = HealthApp(is_ready=runtime.is_ready)

    config = uvicorn.Config(
        health.asgi,
        host="0.0.0.0",
        port=settings.http_port,
        log_level=settings.log_level.lower(),
    )
    http = uvicorn.Server(config)

    await runtime.start()
    try:
        await asyncio.gather(runtime.run_tick_loop(), http.serve())
    finally:
        await runtime.stop()
```

- [ ] **Step 2: Replace `services/opcua-simulator/src/opcua_simulator/__main__.py`**

```python
"""Entry point for the OPC UA simulator pod."""

from __future__ import annotations

import asyncio

from opcua_simulator.config import SimulatorSettings
from opcua_simulator.server import run


def main() -> None:
    settings = SimulatorSettings()
    asyncio.run(run(settings))


if __name__ == "__main__":
    main()
```

- [ ] **Step 3: Write a smoke test `services/opcua-simulator/tests/test_server_smoke.py`**

```python
from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from asyncua import Client

from opcua_simulator.config import SimulatorSettings
from opcua_simulator.server import SimulatorRuntime

REPO_ROOT = Path(__file__).resolve().parents[3]
SAMPLE_AS = REPO_ROOT / "config" / "opcua-address-space.yaml"


@pytest.mark.asyncio
async def test_runtime_start_then_stop_does_not_throw(tmp_path: Path) -> None:
    settings = SimulatorSettings(
        endpoint="opc.tcp://127.0.0.1:54840/eirvah/simulator-test",
        address_space_path=SAMPLE_AS,
        tick_rate_ms=100,
        seed=1,
    )
    runtime = SimulatorRuntime(settings)
    await runtime.start()
    try:
        assert runtime.is_ready() is True
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_setpoint_write_round_trips_through_opcua(tmp_path: Path) -> None:
    settings = SimulatorSettings(
        endpoint="opc.tcp://127.0.0.1:54841/eirvah/simulator-test",
        address_space_path=SAMPLE_AS,
        tick_rate_ms=50,
        seed=2,
    )
    runtime = SimulatorRuntime(settings)
    await runtime.start()
    tick_task = asyncio.create_task(runtime.run_tick_loop())
    try:
        async with Client(url=settings.endpoint) as client:
            ns = await client.get_namespace_index(
                "https://eirvah.uniza/zilina/factory1"
            )
            obj = await client.nodes.objects.get_child([f"{ns}:bottler"])
            sp = await obj.get_child([f"{ns}:SetpointTemperature"])
            await sp.write_value(18.5)
            await asyncio.sleep(0.5)
            value = await sp.read_value()
            assert abs(value - 18.5) < 0.001
    finally:
        tick_task.cancel()
        try:
            await tick_task
        except asyncio.CancelledError:
            pass
        await runtime.stop()
```

- [ ] **Step 4: Run the tests (red on first run, green after `server.py` is in place)**

Run:
```bash
uv run pytest services/opcua-simulator/tests/test_server_smoke.py -v
```
Expected: pass (server.py was written in Step 1).

- [ ] **Step 5: Type-check the whole simulator**

Run:
```bash
uv run mypy services/opcua-simulator/src
```
Expected: no errors.

- [ ] **Step 6: Commit**

```bash
git add services/opcua-simulator/src/opcua_simulator/server.py \
        services/opcua-simulator/src/opcua_simulator/__main__.py \
        services/opcua-simulator/tests/test_server_smoke.py
git commit -m "feat(simulator): OPC UA server + tick loop wiring (spec §9 fully realised)"
```

---

### Task 26: Kustomize root + namespace

**Files:**
- Create: `deploy/k3s/base/kustomization.yaml`
- Create: `deploy/k3s/base/namespace.yaml`

- [ ] **Step 1: Write `deploy/k3s/base/namespace.yaml`**

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: eirvah-edge
  labels:
    eirvah.uniza/component: edge
```

- [ ] **Step 2: Write `deploy/k3s/base/kustomization.yaml`**

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: eirvah-edge
resources:
  - namespace.yaml
  - nats
  - mosquitto
  - rabbitmq
  - prometheus
  - grafana
  - opcua-simulator
labels:
  - includeSelectors: true
    pairs:
      eirvah.uniza/component: edge
```

- [ ] **Step 3: Validate Kustomize builds** (it will fail until the subdirectories exist — expected)

Run:
```bash
kustomize build deploy/k3s/base || echo "expected to fail until later tasks add the directories"
```

- [ ] **Step 4: Commit**

```bash
git add deploy/k3s/base/kustomization.yaml deploy/k3s/base/namespace.yaml
git commit -m "feat(deploy): Kustomize root and edge namespace"
```

---

### Task 27: Kustomize base — NATS

**Files:**
- Create: `deploy/k3s/base/nats/kustomization.yaml`
- Create: `deploy/k3s/base/nats/deployment.yaml`
- Create: `deploy/k3s/base/nats/service.yaml`

NATS server image is Apache-2.0.

- [ ] **Step 1: Write `deploy/k3s/base/nats/kustomization.yaml`**

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - deployment.yaml
  - service.yaml
```

- [ ] **Step 2: Write `deploy/k3s/base/nats/deployment.yaml`**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: nats
  labels: { app.kubernetes.io/name: nats }
spec:
  replicas: 1
  selector:
    matchLabels: { app.kubernetes.io/name: nats }
  template:
    metadata:
      labels: { app.kubernetes.io/name: nats }
    spec:
      containers:
        - name: nats
          image: nats:2.10-alpine
          args:
            - "-m"
            - "8222"
          ports:
            - name: client
              containerPort: 4222
            - name: monitor
              containerPort: 8222
          readinessProbe:
            httpGet: { path: /healthz, port: 8222 }
            initialDelaySeconds: 2
            periodSeconds: 5
          livenessProbe:
            httpGet: { path: /healthz, port: 8222 }
            initialDelaySeconds: 5
            periodSeconds: 10
          resources:
            requests: { cpu: "20m", memory: "64Mi" }
            limits:   { cpu: "200m", memory: "256Mi" }
```

- [ ] **Step 3: Write `deploy/k3s/base/nats/service.yaml`**

```yaml
apiVersion: v1
kind: Service
metadata:
  name: nats
  labels: { app.kubernetes.io/name: nats }
spec:
  selector: { app.kubernetes.io/name: nats }
  ports:
    - name: client
      port: 4222
      targetPort: 4222
    - name: monitor
      port: 8222
      targetPort: 8222
```

- [ ] **Step 4: Validate just this base**

Run:
```bash
kustomize build deploy/k3s/base/nats
```
Expected: prints valid YAML, no errors.

- [ ] **Step 5: Commit**

```bash
git add deploy/k3s/base/nats
git commit -m "feat(deploy): NATS base manifests"
```

---

### Task 28: Kustomize base — Mosquitto

**Files:**
- Create: `deploy/k3s/base/mosquitto/kustomization.yaml`
- Create: `deploy/k3s/base/mosquitto/configmap.yaml`
- Create: `deploy/k3s/base/mosquitto/secret.yaml`
- Create: `deploy/k3s/base/mosquitto/deployment.yaml`
- Create: `deploy/k3s/base/mosquitto/service.yaml`

`eclipse-mosquitto` image is EPL-2.0/EDL-1.0. License-clean.

- [ ] **Step 1: Write `deploy/k3s/base/mosquitto/kustomization.yaml`**

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - configmap.yaml
  - secret.yaml
  - deployment.yaml
  - service.yaml
```

- [ ] **Step 2: Write `deploy/k3s/base/mosquitto/configmap.yaml`**

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: mosquitto-config
data:
  mosquitto.conf: |
    listener 1883 0.0.0.0
    allow_anonymous false
    password_file /mosquitto/passwd/passwd
    persistence false
    log_dest stdout
```

- [ ] **Step 3: Write `deploy/k3s/base/mosquitto/secret.yaml`**

Important: the password is dev-only and intentionally checked in for the `local` overlay; replaced by a sealed secret or external secret manager in `lab` (Plan 4).

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: mosquitto-passwd
type: Opaque
stringData:
  # Generated via: mosquitto_passwd -c -b /tmp/passwd eirvah eirvah-dev-password
  # Then base64'd is NOT applied — Mosquitto expects the hashed file as-is.
  passwd: |
    eirvah:$7$101$abc123def456ghij$KLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyzABCDEF==
```

> Note for the implementer: regenerate this hash locally with `docker run --rm eclipse-mosquitto:2 mosquitto_passwd -c -b /tmp/p eirvah eirvah-dev-password && docker run --rm -v $(pwd):/w eclipse-mosquitto:2 cat /tmp/p` and replace the line above. It must be a real hash; the placeholder above is illustrative.

- [ ] **Step 4: Write `deploy/k3s/base/mosquitto/deployment.yaml`**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mosquitto
  labels: { app.kubernetes.io/name: mosquitto }
spec:
  replicas: 1
  selector:
    matchLabels: { app.kubernetes.io/name: mosquitto }
  template:
    metadata:
      labels: { app.kubernetes.io/name: mosquitto }
    spec:
      containers:
        - name: mosquitto
          image: eclipse-mosquitto:2
          ports:
            - name: mqtt
              containerPort: 1883
          volumeMounts:
            - name: config
              mountPath: /mosquitto/config
            - name: passwd
              mountPath: /mosquitto/passwd
              readOnly: true
          readinessProbe:
            tcpSocket: { port: 1883 }
            initialDelaySeconds: 2
            periodSeconds: 5
          livenessProbe:
            tcpSocket: { port: 1883 }
            initialDelaySeconds: 5
            periodSeconds: 10
          resources:
            requests: { cpu: "20m", memory: "32Mi" }
            limits:   { cpu: "200m", memory: "128Mi" }
      volumes:
        - name: config
          configMap:
            name: mosquitto-config
            items:
              - key: mosquitto.conf
                path: mosquitto.conf
        - name: passwd
          secret:
            secretName: mosquitto-passwd
```

- [ ] **Step 5: Write `deploy/k3s/base/mosquitto/service.yaml`**

```yaml
apiVersion: v1
kind: Service
metadata:
  name: mosquitto
  labels: { app.kubernetes.io/name: mosquitto }
spec:
  selector: { app.kubernetes.io/name: mosquitto }
  ports:
    - name: mqtt
      port: 1883
      targetPort: 1883
```

- [ ] **Step 6: Validate the base**

Run:
```bash
kustomize build deploy/k3s/base/mosquitto
```
Expected: prints valid YAML.

- [ ] **Step 7: Commit**

```bash
git add deploy/k3s/base/mosquitto
git commit -m "feat(deploy): Mosquitto base manifests with dev credentials"
```

---

### Task 29: Kustomize base — RabbitMQ

**Files:**
- Create: `deploy/k3s/base/rabbitmq/kustomization.yaml`
- Create: `deploy/k3s/base/rabbitmq/configmap.yaml`
- Create: `deploy/k3s/base/rabbitmq/deployment.yaml`
- Create: `deploy/k3s/base/rabbitmq/service.yaml`

RabbitMQ image is MPL-2.0. License-clean. We pre-declare the `eirvah.actuation.requests` queue and the `eirvah.actuation.results` exchange via a definitions file mounted from the ConfigMap.

- [ ] **Step 1: Write `deploy/k3s/base/rabbitmq/kustomization.yaml`**

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - configmap.yaml
  - deployment.yaml
  - service.yaml
```

- [ ] **Step 2: Write `deploy/k3s/base/rabbitmq/configmap.yaml`**

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: rabbitmq-config
data:
  rabbitmq.conf: |
    loopback_users.guest = false
    load_definitions = /etc/rabbitmq/definitions.json
    listeners.tcp.default = 5672
    management.tcp.port = 15672
    prometheus.tcp.port = 15692
  enabled_plugins: |
    [rabbitmq_management,rabbitmq_prometheus].
  definitions.json: |
    {
      "users": [
        {"name": "eirvah", "password": "eirvah-dev-password",
         "tags": "administrator"}
      ],
      "vhosts": [{"name": "/"}],
      "permissions": [
        {"user": "eirvah", "vhost": "/", "configure": ".*",
         "write": ".*", "read": ".*"}
      ],
      "queues": [
        {"name": "eirvah.actuation.requests", "vhost": "/",
         "durable": true, "auto_delete": false, "arguments": {}}
      ],
      "exchanges": [
        {"name": "eirvah.actuation.results", "vhost": "/",
         "type": "topic", "durable": true, "auto_delete": false,
         "arguments": {}}
      ],
      "bindings": []
    }
```

- [ ] **Step 3: Write `deploy/k3s/base/rabbitmq/deployment.yaml`**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: rabbitmq
  labels: { app.kubernetes.io/name: rabbitmq }
spec:
  replicas: 1
  selector:
    matchLabels: { app.kubernetes.io/name: rabbitmq }
  template:
    metadata:
      labels: { app.kubernetes.io/name: rabbitmq }
    spec:
      containers:
        - name: rabbitmq
          image: rabbitmq:3.13-management
          ports:
            - name: amqp
              containerPort: 5672
            - name: management
              containerPort: 15672
            - name: prometheus
              containerPort: 15692
          volumeMounts:
            - name: config
              mountPath: /etc/rabbitmq
          readinessProbe:
            exec:
              command: ["rabbitmq-diagnostics", "-q", "ping"]
            initialDelaySeconds: 15
            periodSeconds: 10
          livenessProbe:
            exec:
              command: ["rabbitmq-diagnostics", "-q", "status"]
            initialDelaySeconds: 30
            periodSeconds: 20
          resources:
            requests: { cpu: "100m", memory: "256Mi" }
            limits:   { cpu: "500m", memory: "512Mi" }
      volumes:
        - name: config
          configMap:
            name: rabbitmq-config
            items:
              - { key: rabbitmq.conf,    path: rabbitmq.conf }
              - { key: enabled_plugins,  path: enabled_plugins }
              - { key: definitions.json, path: definitions.json }
```

- [ ] **Step 4: Write `deploy/k3s/base/rabbitmq/service.yaml`**

```yaml
apiVersion: v1
kind: Service
metadata:
  name: rabbitmq
  labels: { app.kubernetes.io/name: rabbitmq }
spec:
  selector: { app.kubernetes.io/name: rabbitmq }
  ports:
    - { name: amqp,       port: 5672,  targetPort: 5672 }
    - { name: management, port: 15672, targetPort: 15672 }
    - { name: prometheus, port: 15692, targetPort: 15692 }
```

- [ ] **Step 5: Validate**

Run:
```bash
kustomize build deploy/k3s/base/rabbitmq
```
Expected: valid YAML.

- [ ] **Step 6: Commit**

```bash
git add deploy/k3s/base/rabbitmq
git commit -m "feat(deploy): RabbitMQ base with pre-declared actuation queue/exchange"
```

---

### Task 30: Kustomize base — Prometheus

**Files:**
- Create: `deploy/k3s/base/prometheus/kustomization.yaml`
- Create: `deploy/k3s/base/prometheus/rbac.yaml`
- Create: `deploy/k3s/base/prometheus/configmap.yaml`
- Create: `deploy/k3s/base/prometheus/deployment.yaml`
- Create: `deploy/k3s/base/prometheus/service.yaml`

Prometheus image is Apache-2.0. We scrape every Pod in the namespace with `eirvah.uniza/scrape: "true"` annotation.

- [ ] **Step 1: Write `deploy/k3s/base/prometheus/kustomization.yaml`**

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - rbac.yaml
  - configmap.yaml
  - deployment.yaml
  - service.yaml
```

- [ ] **Step 2: Write `deploy/k3s/base/prometheus/rbac.yaml`**

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: prometheus
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: prometheus
rules:
  - apiGroups: [""]
    resources: ["pods", "services", "endpoints"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: prometheus
subjects:
  - kind: ServiceAccount
    name: prometheus
roleRef:
  kind: Role
  name: prometheus
  apiGroup: rbac.authorization.k8s.io
```

- [ ] **Step 3: Write `deploy/k3s/base/prometheus/configmap.yaml`**

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: prometheus-config
data:
  prometheus.yml: |
    global:
      scrape_interval: 5s
      evaluation_interval: 30s
    scrape_configs:
      - job_name: kubernetes-pods
        kubernetes_sd_configs:
          - role: pod
            namespaces:
              names: [eirvah-edge]
        relabel_configs:
          - source_labels: [__meta_kubernetes_pod_annotation_eirvah_uniza_scrape]
            action: keep
            regex: "true"
          - source_labels: [__meta_kubernetes_pod_annotation_eirvah_uniza_scrape_port]
            action: replace
            regex: (\d+)
            target_label: __address__
            replacement: $1
            source_labels: [__address__, __meta_kubernetes_pod_annotation_eirvah_uniza_scrape_port]
            separator: ":"
            regex: ([^:]+):(.*);(.*)
            target_label: __address__
            replacement: $1:$3
          - source_labels: [__meta_kubernetes_pod_name]
            target_label: pod
          - source_labels: [__meta_kubernetes_pod_label_app_kubernetes_io_name]
            target_label: app
      - job_name: rabbitmq
        static_configs:
          - targets: ["rabbitmq.eirvah-edge.svc.cluster.local:15692"]
      - job_name: nats
        static_configs:
          - targets: ["nats.eirvah-edge.svc.cluster.local:8222"]
```

- [ ] **Step 4: Write `deploy/k3s/base/prometheus/deployment.yaml`**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: prometheus
  labels: { app.kubernetes.io/name: prometheus }
spec:
  replicas: 1
  selector:
    matchLabels: { app.kubernetes.io/name: prometheus }
  template:
    metadata:
      labels: { app.kubernetes.io/name: prometheus }
    spec:
      serviceAccountName: prometheus
      containers:
        - name: prometheus
          image: prom/prometheus:v2.54.1
          args:
            - "--config.file=/etc/prometheus/prometheus.yml"
            - "--storage.tsdb.path=/prometheus"
            - "--storage.tsdb.retention.time=1h"
            - "--web.enable-lifecycle"
          ports:
            - name: http
              containerPort: 9090
          volumeMounts:
            - name: config
              mountPath: /etc/prometheus
          readinessProbe:
            httpGet: { path: /-/ready, port: 9090 }
            initialDelaySeconds: 5
            periodSeconds: 5
          livenessProbe:
            httpGet: { path: /-/healthy, port: 9090 }
            initialDelaySeconds: 10
            periodSeconds: 10
          resources:
            requests: { cpu: "50m", memory: "128Mi" }
            limits:   { cpu: "500m", memory: "512Mi" }
      volumes:
        - name: config
          configMap:
            name: prometheus-config
```

- [ ] **Step 5: Write `deploy/k3s/base/prometheus/service.yaml`**

```yaml
apiVersion: v1
kind: Service
metadata:
  name: prometheus
  labels: { app.kubernetes.io/name: prometheus }
spec:
  selector: { app.kubernetes.io/name: prometheus }
  ports:
    - { name: http, port: 9090, targetPort: 9090 }
```

- [ ] **Step 6: Validate and commit**

Run:
```bash
kustomize build deploy/k3s/base/prometheus
```
Expected: valid YAML.

```bash
git add deploy/k3s/base/prometheus
git commit -m "feat(deploy): Prometheus base with annotation-driven pod scrape"
```

---

### Task 31: Kustomize base — Grafana

**Files:**
- Create: `deploy/k3s/base/grafana/kustomization.yaml`
- Create: `deploy/k3s/base/grafana/secret.yaml`
- Create: `deploy/k3s/base/grafana/configmap-datasources.yaml`
- Create: `deploy/k3s/base/grafana/configmap-dashboards-provisioning.yaml`
- Create: `deploy/k3s/base/grafana/deployment.yaml`
- Create: `deploy/k3s/base/grafana/service.yaml`

The dashboard JSON itself is added in Task 32 and referenced by this configmap-dashboards-provisioning.

- [ ] **Step 1: Write `deploy/k3s/base/grafana/kustomization.yaml`**

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - secret.yaml
  - configmap-datasources.yaml
  - configmap-dashboards-provisioning.yaml
  - deployment.yaml
  - service.yaml

configMapGenerator:
  - name: grafana-dashboards
    files:
      - ../../../grafana/dashboards/bottling-line-state.json
```

- [ ] **Step 2: Write `deploy/k3s/base/grafana/secret.yaml`**

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: grafana-admin
type: Opaque
stringData:
  admin-user: admin
  admin-password: eirvah-dev-grafana
```

- [ ] **Step 3: Write `deploy/k3s/base/grafana/configmap-datasources.yaml`**

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: grafana-datasources
data:
  datasources.yaml: |
    apiVersion: 1
    datasources:
      - name: Prometheus
        type: prometheus
        access: proxy
        url: http://prometheus.eirvah-edge.svc.cluster.local:9090
        isDefault: true
        editable: false
```

- [ ] **Step 4: Write `deploy/k3s/base/grafana/configmap-dashboards-provisioning.yaml`**

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: grafana-dashboards-provisioning
data:
  dashboards.yaml: |
    apiVersion: 1
    providers:
      - name: EirVah
        orgId: 1
        folder: EirVah
        type: file
        disableDeletion: true
        editable: false
        options:
          path: /var/lib/grafana/dashboards
```

- [ ] **Step 5: Write `deploy/k3s/base/grafana/deployment.yaml`**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: grafana
  labels: { app.kubernetes.io/name: grafana }
spec:
  replicas: 1
  selector:
    matchLabels: { app.kubernetes.io/name: grafana }
  template:
    metadata:
      labels: { app.kubernetes.io/name: grafana }
    spec:
      containers:
        - name: grafana
          image: grafana/grafana:11.2.2
          env:
            - name: GF_SECURITY_ADMIN_USER
              valueFrom:
                secretKeyRef: { name: grafana-admin, key: admin-user }
            - name: GF_SECURITY_ADMIN_PASSWORD
              valueFrom:
                secretKeyRef: { name: grafana-admin, key: admin-password }
            - name: GF_ANALYTICS_REPORTING_ENABLED
              value: "false"
            - name: GF_AUTH_ANONYMOUS_ENABLED
              value: "false"
          ports:
            - name: http
              containerPort: 3000
          volumeMounts:
            - name: datasources
              mountPath: /etc/grafana/provisioning/datasources
            - name: dash-provisioning
              mountPath: /etc/grafana/provisioning/dashboards
            - name: dashboards
              mountPath: /var/lib/grafana/dashboards
          readinessProbe:
            httpGet: { path: /api/health, port: 3000 }
            initialDelaySeconds: 5
            periodSeconds: 5
          livenessProbe:
            httpGet: { path: /api/health, port: 3000 }
            initialDelaySeconds: 30
            periodSeconds: 30
          resources:
            requests: { cpu: "50m", memory: "128Mi" }
            limits:   { cpu: "500m", memory: "512Mi" }
      volumes:
        - name: datasources
          configMap: { name: grafana-datasources }
        - name: dash-provisioning
          configMap: { name: grafana-dashboards-provisioning }
        - name: dashboards
          configMap: { name: grafana-dashboards }
```

- [ ] **Step 6: Write `deploy/k3s/base/grafana/service.yaml`**

```yaml
apiVersion: v1
kind: Service
metadata:
  name: grafana
  labels: { app.kubernetes.io/name: grafana }
spec:
  selector: { app.kubernetes.io/name: grafana }
  ports:
    - { name: http, port: 3000, targetPort: 3000 }
```

- [ ] **Step 7: Commit**

```bash
git add deploy/k3s/base/grafana
git commit -m "feat(deploy): Grafana base with provisioned datasource and dashboard slot"
```

---

### Task 32: "Bottling Line State" dashboard JSON

**Files:**
- Create: `deploy/grafana/dashboards/bottling-line-state.json`

Concrete dashboard JSON. Six panels (spec §6.7 Dashboard 2): temperature + setpoint overlay, setpoint stat, motor state stat, motor RPM line, throughput line, recent setpoint writes table.

- [ ] **Step 1: Write `deploy/grafana/dashboards/bottling-line-state.json`**

```json
{
  "uid": "bottling-line-state",
  "title": "Bottling Line State",
  "schemaVersion": 39,
  "version": 1,
  "tags": ["eirvah", "device-truth"],
  "timezone": "browser",
  "time": { "from": "now-15m", "to": "now" },
  "refresh": "5s",
  "panels": [
    {
      "id": 1,
      "type": "timeseries",
      "title": "Temperature vs setpoint",
      "gridPos": { "x": 0, "y": 0, "w": 12, "h": 8 },
      "targets": [
        {
          "expr": "eirvah_simulator_temperature_celsius",
          "legendFormat": "temperature {{equipment}}",
          "refId": "T"
        },
        {
          "expr": "eirvah_simulator_setpoint_celsius",
          "legendFormat": "setpoint {{equipment}}",
          "refId": "S"
        }
      ],
      "fieldConfig": {
        "defaults": { "unit": "celsius" }
      }
    },
    {
      "id": 2,
      "type": "stat",
      "title": "Current setpoint (°C)",
      "gridPos": { "x": 12, "y": 0, "w": 6, "h": 4 },
      "targets": [
        { "expr": "eirvah_simulator_setpoint_celsius", "refId": "A" }
      ],
      "fieldConfig": { "defaults": { "unit": "celsius" } }
    },
    {
      "id": 3,
      "type": "stat",
      "title": "Motor state",
      "gridPos": { "x": 18, "y": 0, "w": 6, "h": 4 },
      "targets": [
        { "expr": "eirvah_simulator_motor_state", "refId": "A" }
      ],
      "fieldConfig": {
        "defaults": {
          "mappings": [
            { "type": "value", "options": {
              "0": { "text": "stopped", "color": "gray" },
              "1": { "text": "starting", "color": "yellow" },
              "2": { "text": "running", "color": "green" },
              "3": { "text": "fault", "color": "red" }
            }}
          ]
        }
      }
    },
    {
      "id": 4,
      "type": "timeseries",
      "title": "Motor RPM",
      "gridPos": { "x": 12, "y": 4, "w": 6, "h": 4 },
      "targets": [
        { "expr": "eirvah_simulator_motor_rpm", "refId": "A" }
      ],
      "fieldConfig": { "defaults": { "unit": "rotrpm" } }
    },
    {
      "id": 5,
      "type": "timeseries",
      "title": "Throughput (bottles/s)",
      "gridPos": { "x": 18, "y": 4, "w": 6, "h": 4 },
      "targets": [
        { "expr": "eirvah_simulator_throughput_bottles_per_second", "refId": "A" }
      ]
    },
    {
      "id": 6,
      "type": "table",
      "title": "Setpoint writes (cumulative)",
      "gridPos": { "x": 0, "y": 8, "w": 24, "h": 6 },
      "targets": [
        {
          "expr": "eirvah_simulator_setpoint_writes_total",
          "legendFormat": "{{writer}}",
          "refId": "A",
          "instant": true,
          "format": "table"
        }
      ]
    }
  ]
}
```

- [ ] **Step 2: Commit**

```bash
git add deploy/grafana/dashboards/bottling-line-state.json
git commit -m "feat(deploy): Bottling Line State Grafana dashboard JSON"
```

---

### Task 33: Kustomize base — opcua-simulator

**Files:**
- Create: `deploy/k3s/base/opcua-simulator/kustomization.yaml`
- Create: `deploy/k3s/base/opcua-simulator/configmap.yaml`
- Create: `deploy/k3s/base/opcua-simulator/deployment.yaml`
- Create: `deploy/k3s/base/opcua-simulator/service.yaml`

- [ ] **Step 1: Write `deploy/k3s/base/opcua-simulator/kustomization.yaml`**

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - configmap.yaml
  - deployment.yaml
  - service.yaml

configMapGenerator:
  - name: opcua-simulator-address-space
    files:
      - ../../../../config/opcua-address-space.yaml
```

- [ ] **Step 2: Write `deploy/k3s/base/opcua-simulator/configmap.yaml`**

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: opcua-simulator-env
data:
  OPCUA_SIMULATOR_ENDPOINT: "opc.tcp://0.0.0.0:4840/eirvah/simulator"
  OPCUA_SIMULATOR_TICK_RATE_MS: "100"
  OPCUA_SIMULATOR_SEED: "1"
  OPCUA_SIMULATOR_ADDRESS_SPACE_PATH: "/etc/opcua-simulator/opcua-address-space.yaml"
  OPCUA_SIMULATOR_HTTP_PORT: "8080"
  OPCUA_SIMULATOR_HOT_SPIKE_PROBABILITY: "0.005"
  OPCUA_SIMULATOR_MOTOR_FAULT_PROBABILITY: "0.0"
  OPCUA_SIMULATOR_LOG_LEVEL: "INFO"
```

- [ ] **Step 3: Write `deploy/k3s/base/opcua-simulator/deployment.yaml`**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: opcua-simulator
  labels: { app.kubernetes.io/name: opcua-simulator }
spec:
  replicas: 1
  selector:
    matchLabels: { app.kubernetes.io/name: opcua-simulator }
  template:
    metadata:
      labels: { app.kubernetes.io/name: opcua-simulator }
      annotations:
        eirvah.uniza/scrape: "true"
        eirvah.uniza/scrape-port: "8080"
    spec:
      containers:
        - name: opcua-simulator
          image: opcua-simulator:local
          imagePullPolicy: IfNotPresent
          envFrom:
            - configMapRef: { name: opcua-simulator-env }
          ports:
            - { name: opcua, containerPort: 4840 }
            - { name: http,  containerPort: 8080 }
          volumeMounts:
            - name: address-space
              mountPath: /etc/opcua-simulator
              readOnly: true
          readinessProbe:
            httpGet: { path: /readyz, port: 8080 }
            initialDelaySeconds: 3
            periodSeconds: 5
          livenessProbe:
            httpGet: { path: /healthz, port: 8080 }
            initialDelaySeconds: 10
            periodSeconds: 10
          resources:
            requests: { cpu: "50m", memory: "128Mi" }
            limits:   { cpu: "500m", memory: "256Mi" }
      volumes:
        - name: address-space
          configMap:
            name: opcua-simulator-address-space
```

- [ ] **Step 4: Write `deploy/k3s/base/opcua-simulator/service.yaml`**

```yaml
apiVersion: v1
kind: Service
metadata:
  name: opcua-simulator
  labels: { app.kubernetes.io/name: opcua-simulator }
spec:
  selector: { app.kubernetes.io/name: opcua-simulator }
  ports:
    - { name: opcua, port: 4840, targetPort: 4840 }
    - { name: http,  port: 8080, targetPort: 8080 }
```

- [ ] **Step 5: Validate the whole base**

Run:
```bash
kustomize build deploy/k3s/base > /tmp/eirvah-base.yaml
wc -l /tmp/eirvah-base.yaml
```
Expected: prints a non-trivial line count; no kustomize errors.

- [ ] **Step 6: Commit**

```bash
git add deploy/k3s/base/opcua-simulator
git commit -m "feat(deploy): opcua-simulator base manifests with Prometheus scrape annotation"
```

---

### Task 34: `local` overlay

**Files:**
- Create: `deploy/k3s/overlays/local/kustomization.yaml`

The `local` overlay for Plan 1 is intentionally minimal — it just references the base unchanged. Later plans add per-service patches here (and `lab` overlay adds resource sizing and image SHA pinning).

- [ ] **Step 1: Write `deploy/k3s/overlays/local/kustomization.yaml`**

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: eirvah-edge
resources:
  - ../../base
```

- [ ] **Step 2: Validate**

Run:
```bash
kustomize build deploy/k3s/overlays/local | head -50
```
Expected: valid YAML starting with `apiVersion: v1\nkind: Namespace`.

- [ ] **Step 3: Commit**

```bash
git add deploy/k3s/overlays/local
git commit -m "feat(deploy): local overlay referencing the unmodified base"
```

---

### Task 35: `dev_up.sh`, `dev_down.sh`, `build_all.sh`

**Files:**
- Create: `scripts/dev_up.sh`
- Create: `scripts/dev_down.sh`
- Create: `scripts/build_all.sh`

- [ ] **Step 1: Write `scripts/build_all.sh`**

```bash
#!/usr/bin/env bash
# Build every service image with the local Docker daemon.
# Usage: scripts/build_all.sh [<git-short-sha>]
# If no SHA is provided, tags as ':local'.

set -euo pipefail

cd "$(dirname "$0")/.."

TAG="${1:-local}"

SERVICES=(
  opcua-simulator
)

for svc in "${SERVICES[@]}"; do
  echo "==> building ${svc}:${TAG}"
  docker build \
    --file "services/${svc}/Dockerfile" \
    --tag "${svc}:${TAG}" \
    .
done
```

- [ ] **Step 2: Write `scripts/dev_up.sh`**

```bash
#!/usr/bin/env bash
# Create the local k3d cluster, build images, import, apply the local overlay.
# Idempotent: rerunning rebuilds images and reapplies manifests.

set -euo pipefail

cd "$(dirname "$0")/.."

CLUSTER="eirvah-edge"
NAMESPACE="eirvah-edge"
SERVICES=(
  opcua-simulator
)

# 1. Cluster
if ! k3d cluster list | awk '{print $1}' | grep -qx "$CLUSTER"; then
  echo "==> creating k3d cluster '${CLUSTER}'"
  k3d cluster create "${CLUSTER}" \
    --port "3000:30000@server:0" \
    --port "9090:30090@server:0" \
    --port "1883:31883@server:0" \
    --port "5672:35672@server:0" \
    --port "15672:35672@server:0" \
    --port "4840:34840@server:0" \
    --wait
else
  echo "==> k3d cluster '${CLUSTER}' already exists"
fi

# 2. Build + import images
./scripts/build_all.sh local
for svc in "${SERVICES[@]}"; do
  echo "==> importing ${svc}:local"
  k3d image import "${svc}:local" --cluster "${CLUSTER}"
done

# 3. Apply manifests
echo "==> applying overlay deploy/k3s/overlays/local"
kubectl apply -k deploy/k3s/overlays/local

# 4. Wait for readiness
echo "==> waiting for deployments to become available"
kubectl -n "${NAMESPACE}" wait --for=condition=Available --timeout=180s deployment --all

# 5. Hints
GRAFANA_NODE_PORT=$(kubectl -n "${NAMESPACE}" get svc grafana \
  -o jsonpath='{.spec.ports[?(@.name=="http")].nodePort}' 2>/dev/null || echo "")
echo
echo "==> stack is up."
echo "    Grafana:    kubectl -n ${NAMESPACE} port-forward svc/grafana 3000:3000"
echo "    Prometheus: kubectl -n ${NAMESPACE} port-forward svc/prometheus 9090:9090"
echo "    OPC UA:     kubectl -n ${NAMESPACE} port-forward svc/opcua-simulator 4840:4840"
echo "    Mosquitto:  kubectl -n ${NAMESPACE} port-forward svc/mosquitto 1883:1883"
echo "    RabbitMQ:   kubectl -n ${NAMESPACE} port-forward svc/rabbitmq 15672:15672"
echo "    Grafana credentials: admin / eirvah-dev-grafana"
echo "    Open the 'Bottling Line State' dashboard once you've port-forwarded Grafana."
```

- [ ] **Step 3: Write `scripts/dev_down.sh`**

```bash
#!/usr/bin/env bash
# Delete the local k3d cluster created by dev_up.sh.

set -euo pipefail

CLUSTER="eirvah-edge"

if k3d cluster list | awk '{print $1}' | grep -qx "${CLUSTER}"; then
  echo "==> deleting k3d cluster '${CLUSTER}'"
  k3d cluster delete "${CLUSTER}"
else
  echo "==> no k3d cluster named '${CLUSTER}' found; nothing to do"
fi
```

- [ ] **Step 4: Make the scripts executable**

Run:
```bash
chmod +x scripts/dev_up.sh scripts/dev_down.sh scripts/build_all.sh
```

- [ ] **Step 5: Shellcheck (best-effort)**

Run:
```bash
shellcheck scripts/*.sh || echo "(shellcheck not installed — non-blocking)"
```

- [ ] **Step 6: Commit**

```bash
git add scripts/
git commit -m "feat(scripts): dev_up / dev_down / build_all driving k3d + kustomize"
```

---

### Task 36: Plan 1 acceptance smoke test (manual)

This is the gate that proves Plan 1 is done. It's a runbook, not a pytest. The implementer follows each step and confirms the expected outcome.

**Files:**
- None created. Optionally tags a release at the end.

- [ ] **Step 1: Bring the cluster up cold**

```bash
./scripts/dev_down.sh || true
./scripts/dev_up.sh
```
Expected: script exits 0; final line lists the port-forward hints.

- [ ] **Step 2: Confirm all deployments are Ready**

```bash
kubectl -n eirvah-edge get deploy
```
Expected: six deployments — `nats`, `mosquitto`, `rabbitmq`, `prometheus`, `grafana`, `opcua-simulator` — each `READY` showing `1/1`.

- [ ] **Step 3: Confirm the simulator's `/metrics` exposes EirVah gauges**

```bash
kubectl -n eirvah-edge port-forward svc/opcua-simulator 8080:8080 >/tmp/pf.log 2>&1 &
PF=$!
sleep 2
curl -s http://localhost:8080/metrics | grep '^eirvah_simulator_' | head
kill ${PF}
```
Expected: lines like
```
eirvah_simulator_temperature_celsius{...} 22.x
eirvah_simulator_setpoint_celsius{...} 22.0
eirvah_simulator_motor_state{...} 0
...
```

- [ ] **Step 4: Confirm Prometheus is scraping the simulator**

```bash
kubectl -n eirvah-edge port-forward svc/prometheus 9090:9090 >/tmp/pf.log 2>&1 &
PF=$!
sleep 2
curl -s 'http://localhost:9090/api/v1/query?query=eirvah_simulator_temperature_celsius' \
  | python -m json.tool | head
kill ${PF}
```
Expected: JSON `data.result` non-empty.

- [ ] **Step 5: Open Grafana and inspect the "Bottling Line State" dashboard**

```bash
kubectl -n eirvah-edge port-forward svc/grafana 3000:3000
```
Open `http://localhost:3000`, log in as `admin` / `eirvah-dev-grafana`. Navigate to **Dashboards → EirVah → Bottling Line State**.

Expected:
- Temperature time-series shows a line near 22 °C with mild noise; setpoint line flat at 22.0.
- Motor state stat shows `stopped` initially, transitions to `starting` (~5 s) then `running` (~8 s).
- Throughput rises from 0 to ~0.9 once the motor is running.
- If you wait long enough, the stochastic hot-spike fires occasionally — temperature line jumps then decays.

- [ ] **Step 6: Trigger a hot spike via the OPC UA method and watch the dashboard react**

```bash
kubectl -n eirvah-edge port-forward svc/opcua-simulator 4840:4840 &
PF=$!
sleep 2
# Use opcua-commander (GPL-3.0) interactively, OR an inline asyncua script:
uv run python - <<'PY'
import asyncio
from asyncua import Client

async def main() -> None:
    async with Client(url="opc.tcp://localhost:4840/eirvah/simulator") as c:
        ns = await c.get_namespace_index("https://eirvah.uniza/zilina/factory1")
        bottler = await c.nodes.objects.get_child([f"{ns}:bottler"])
        method = await bottler.get_child([f"{ns}:TriggerHotSpike"])
        await bottler.call_method(method)
        print("hot spike triggered")

asyncio.run(main())
PY
kill ${PF}
```
Expected: temperature line jumps by ~5 °C in Grafana within a few seconds, then decays.

- [ ] **Step 7: Tear it down and bring it back up; confirm idempotence**

```bash
./scripts/dev_down.sh
./scripts/dev_up.sh
```
Expected: same outcome as the first `dev_up.sh`; no manual steps required.

- [ ] **Step 8: Run the entire unit-test + type-check + lint suite to confirm nothing regressed**

```bash
uv run ruff check .
uv run mypy libs services
uv run pytest libs services
```
Expected: all three commands exit 0.

- [ ] **Step 9: Tag the commit so future plans can reference it**

```bash
git tag -a plan-1-complete -m "Plan 1 Foundations complete: simulator + brokers + observability on k3d."
git push origin plan-1-complete
```

- [ ] **Step 10: Mark this plan complete**

In `README.md`, replace `**Plan 1 — Foundations:** in progress` with:
```
- **Plan 1 — Foundations:** complete (`plan-1-complete` tag).
```

Commit:
```bash
git add README.md
git commit -m "docs(repo): mark Plan 1 complete"
git push
```

---

## Plan 1 self-review

After the implementer finishes Task 36, this is the checklist that closes the plan out.

**1. Spec coverage (Plan 1 scope):**
- §3.3 `opcua-simulator` description — Tasks 15, 16, 24, 25.
- §6.7 Dashboard 2 "Bottling Line State" — Task 32, validated in Task 36.
- §9.1 Address space — Task 16 (sample YAML).
- §9.2 Dynamics (temperature, motor, throughput, setpoint writes, hot-spike) — Tasks 18, 19, 20, 21, 22.
- §9.3 Quality codes — Task 23.
- §9.4 Determinism (seeded PRNG) — Task 17.
- §9.5 Observability of simulator state — Tasks 24, 25, 30 (Prometheus scrape), 32 (dashboard).
- §11 acceptance criteria #5 (`local` defaults `allow_writes: false`) — N/A in Plan 1 (no actuation orchestrator yet); Plan 3 enforces.
- §11 acceptance criterion #6 (every dependency OSI-approved) — every license is called out the first time a new dep appears. Plan 4 adds the automated audit.
- §11 acceptance criterion #7 (reviewer-reproducible baseline) — partial: `dev_up.sh` works; full reproduction requires Plans 2 and 3.

**2. Placeholders:** none. Every step shows actual code or actual command.

**3. Type consistency:**
- `RequestTimeout` defined in Task 10, used implicitly by orchestrator tests in Plan 2.
- `SimulatorMetrics` API methods (`set_temperature`, `set_setpoint`, …) used consistently across Task 24 (definition), Task 25 (usage).
- `NodeDefinition.kind` and `.dynamics` fields are checked by name in `server.py` (Task 25) exactly as defined in `address_space.py` (Task 16).
- `MOTOR_STOPPED` / `MOTOR_STARTING` / `MOTOR_RUNNING` / `MOTOR_FAULT` are imported from `opcua_simulator.motor` in `throughput.py` (Task 20) and `server.py` (Task 25), matching definitions in Task 19.

---

## Execution handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-16-plan-1-foundations.md`. Two execution options:

1. **Subagent-Driven (recommended)** — dispatch a fresh subagent per task, review between tasks, fast iteration.
2. **Inline Execution** — execute tasks in this session using `executing-plans`, batch execution with checkpoints.

Which approach?
