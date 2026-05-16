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
