# ADR 0002 — Reverse Mapping via Shared ConfigMap

**Date:** 2026-05-18
**Status:** Accepted

## Context

`actuation-signal-publisher` must resolve a UNS topic (e.g., `uniza/zilina/factory1/line_a/bottler/setpoint_unit/setpoint_temperature`) to an OPC UA browse path for the write operation. Two options were considered:

**Option A:** Maintain a separate reverse mapping file (`uns-to-opcua-mapping.yaml`) alongside the existing `opcua-node-to-uns-mapping.yaml`.

**Option B:** Invert `opcua-node-to-uns-mapping.yaml` at runtime and load `opcua-node-list.yaml` for browse paths. Both files already exist and are used by the telemetry path.

## Decision

Option B — invert at runtime.

Reasons:
- Single source of truth. A separate reverse file would drift from the forward mapping as nodes are added.
- `opcua-node-to-uns-mapping.yaml` already defines the canonical relationship; the reverse is derived, not independently authoritative.
- `opcua-node-list.yaml` already contains browse paths; duplicating them would create a third file that also drifts.

## Constraints imposed

The mapping must be **bijective**: each UNS topic must map to exactly one `node_id` alias and each alias to exactly one UNS topic. `actuation-signal-publisher` enforces this at startup with a `ValueError` and fails fast if duplicates are found.

For the bottling-line slice this is a safe constraint — one physical line, no repeated measurements. If a future protocol adapter introduces duplicate UNS topics (e.g., redundant sensors), a dedicated reverse mapping file or a tiebreaker field should be introduced.

## Consequences

- `actuation-signal-publisher` mounts two ConfigMaps: `opcua-node-to-uns-mapping.yaml` and `opcua-node-list.yaml`.
- Adding a new writable node requires updating only `opcua-node-to-uns-mapping.yaml` and `opcua-node-list.yaml` — no additional file.
- Startup failure on non-bijective mapping is intentional; it surfaces misconfiguration early.
