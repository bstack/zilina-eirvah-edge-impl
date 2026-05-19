# ADR 0001 — Actuation Safety Gate

**Date:** 2026-05-18
**Status:** Accepted

## Context

The actuation path writes values back to a physical device (OPC UA setpoint). CPS writes carry physical risk: an unintended setpoint change in a real bottling line can damage equipment or product. The slice must run safely on a developer laptop, in CI, and in a shared lab environment without any risk of unintended writes.

## Decision

Implement a two-layer safety gate:

1. **`ALLOW_WRITES` feature flag** (default `false`) on `actuation-control-orchestrator`. When `false`, the pipeline validates the request but short-circuits before calling `act.work.write_signal`, emitting a rejection with reason `writes_disabled`. No OPC UA write occurs regardless of what the validator decides.

2. **`actuation-event-validator` policy bounds check** (value range + allowlist). Runs regardless of the flag. Prevents unsafe writes even when the flag is enabled.

Neither layer alone is sufficient:
- The flag prevents writes in safe environments but would bypass policy checks if the only gate.
- The validator enforces policy but has no awareness of environment safety.

Together: `allow_writes=true` is an explicit, deliberate act that still cannot bypass policy.

## Consequences

- Full-loop e2e tests require `ALLOW_WRITES=true`. CI defaults to `false`.
- Lab and production k3s overlays set the flag explicitly.
- The safety default means "run everything, prove validation works, but don't touch hardware" — appropriate for a slice whose primary purpose is architectural demonstration.
