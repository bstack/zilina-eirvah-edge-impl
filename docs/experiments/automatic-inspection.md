# Automatic sampled bottle inspection and rejection

## Local functional scope

The existing five controls remain: temperature, conveyor, filler, capper and bottle rejection. This stage closes the item-specific path:

`actual simulated bottle -> sampled inspection -> Modbus snapshot -> NATS conversion/contextualization -> MQTT UNS observation -> decision -> AMQP validation/write -> Modbus rejection -> acknowledgement and UNS feedback`

The warmer's recipe rule continues independently. No new service or third-party dependency is introduced. Inspection is a deterministic simulated pass/fail decision, not machine vision or a validated model of manufacturing defects.

## Inspection pattern and identity

By default the Modbus simulator inspects every fourth newly observed conveyor bottle and marks every fifth inspection as failed. At the default 500 ms tick this produces an inspection roughly every two seconds and a failed inspection roughly every ten seconds while the conveyor moves. These are configurable engineering workload assumptions, not measured industrial defect rates. Stopped or faulted conveyors produce no new bottles or inspections. The existing conveyor jam behaviour remains; reset and start it explicitly if jammed.

- `MODBUS_SIMULATOR_INSPECTION_EVERY_N_BOTTLES`: default 4.
- `MODBUS_SIMULATOR_INSPECTION_FAIL_EVERY_N`: default 5.

Each inspection carries schema version 1.0, an inspection ID, actual bottle ID, pass/fail and the original UTC inspection timestamp. Its identity combines a random 64-bit simulator-session value and a 16-bit inspection sequence. Sequence rollover advances the session value. The pass/fail and sampled-bottle pattern is repeatable; session IDs and wall-clock timestamps intentionally differ across runs.

Bottle IDs remain the simulator's existing 1-65535 IDs and the rejector accepts only the recent 32-bottle window. They are not globally unique business identities. The inspection decision budget is two seconds by default, configured with `DECISION_AGENT_STUB_INSPECTION_MAX_AGE_S` (up to five seconds). It assumes synchronized clocks and must be reconsidered if tick rate, delivery delay or placement changes. Device-level session binding, durable idempotency and recovery across simulator restarts remain later command-contract work.

## Atomic snapshot and delivery limits

Holding registers 32-41 contain one snapshot: four words for the 64-bit session, sequence, bottle ID, status (0 absent, 1 pass, 2 fail), and three words for the original epoch-millisecond timestamp. The simulator writes these words together, without yielding between fields. The adapter obtains them in the same bulk read as the regular device feedback; it does not join separately arriving bottle and pass/fail observations.

The adapter emits `Inspector01.BottleInspection` as one JSON string through the existing raw-signal pipeline. It preserves the original inspection timestamp even when polling republishes the same snapshot. The contextualizer maps it to:

`uniza/zilina/factory1/line_a/inspector/inspector_01/bottle_inspection`

The public SOSA observation has unit `json`; its `sosa:hasSimpleResult` contains the versioned JSON inspection result. The old OPC UA aggregate `Inspector01.GoodRate` remains an independent demonstration signal and does not determine these bottle-specific results.

This is sampled, latest-snapshot delivery, not a durable event queue. Disconnections or polling slower than inspection production can skip results. Within an adapter session, detected inspection-sequence gaps are logged as `inspection_snapshot_gap`; initial/restart coverage is unknown. There is no claim that every bottle is inspected or every generated inspection is delivered. Do not infer zero gaps from the absence of a gap warning.

## Decision and duplicate handling

The decision service subscribes to the inspection topic and existing rejected-bottle/count feedback. It waits for a fresh rejection-count baseline before submitting work. Good bottles generate no rejection command. Failed inspections generate a fresh correlated command targeting their bottle ID, through the normal AMQP queue, explicit validator allowlist and existing Modbus writer. The new allowlist permission applies only to the reject route.

The service ignores bad-quality, invalid, future or stale inputs, inconsistent inner/outer inspection timestamps and wrong units. In-memory deduplication covers both inspection identity and session/bottle identity, retains entries for the freshness budget plus ten seconds, and fails closed at capacity instead of evicting live entries. Repeat polling or MQTT redelivery does not produce another request within this window. The device also rejects another rejection of the same bottle in its retained window, without incrementing the rejection count again.

The decision deployment uses one replica and the `Recreate` strategy: the old pod stops before its replacement starts. This prevents normal rolling-update overlap from creating concurrent decision agents. There is a decision-service interruption during replacement; the latest-snapshot input can miss inspections during this gap. Keep the decision service at one replica until coordinated ownership is implemented.

This is not durable exactly-once processing. Decision-service restart clears its in-memory state; a still-fresh inspection can be submitted again and is then subject to device duplicate protection. No automatic retry is made after ambiguous outcomes. A lost result or command failure must be investigated rather than silently retried.

`DECISION_AGENT_STUB_INSPECTION_ENABLED=false` disables automatic rejection independently of `DECISION_AGENT_STUB_ENABLED`, which controls the warmer rule. Disable automatic rejection when running isolated manual rejector tests.

## Confirmation and uncertainty

The decision service creates its AMQP result queue before consuming observations and tracks a bounded set of pending rejection commands. A rejection becomes `confirmed` only when it has:

1. A correlated application approval with `written_at`, representing the writer's device acknowledgement.
2. Rejected-bottle feedback matching the requested bottle, observed after submission.
3. Rejection-count feedback above the fresh pre-command baseline, also observed after submission.

The counter supports evidence that a rejection occurred; concurrent manual actions can also change it, so it alone does not prove attribution or exactly one physical action. Loopback tests separately verify that duplicate requests leave the count unchanged.

Pipeline reject/error results are reported as `not_confirmed`, not proof that no write occurred: the existing pipeline can label write timeouts as rejection. Missing evidence expires to `unknown` ten seconds after the command deadline when subsequent traffic/results are processed. Pending work is never automatically retried. If a later rejection overwrites the last-bottle feedback before it is observed, confirmation may remain unknown even if the first rejection happened. No claim of exactly-once physical effect is made. Counter saturation prevents new automatic submissions because a further increment cannot be observed.

## Operator checks

Rebuild/load `modbus-simulator`, `modbus-data-subscriber` and `decision-agent-stub`. Apply the local overlay to update inspection ingestion, conversion rules, ontology, validator policy and Grafana; then restart the three rebuilt deployments and wait for rollout. ConfigMap references also roll the converter, contextualizer, validator and Grafana as applicable. No cluster recreation or native-writer code change is needed. This stage does not deploy automatically.

With the conveyor running, leave manual commands and disturbance tooling stopped for the initial rejection-only check. Watch the **Automatic bottle inspection and rejection** row on **Bottling Line State**. Sampled inspections should rise more frequently than failed inspections; rejections should follow the failed subset. The existing last-rejected-bottle tile shows the actual rejected ID. These are development diagnostics, not final paper evidence.

```powershell
kubectl --context kind-eirvah-edge -n eirvah-edge logs deployment/modbus-data-subscriber --since=3m | Select-String 'bottle_inspection_observed|inspection_snapshot_gap'
kubectl --context kind-eirvah-edge -n eirvah-edge logs deployment/decision-agent-stub --since=3m | Select-String 'automatic_rejection_requested|automatic_rejection_outcome'
```

Expected outcomes are `confirmed` for ordinary failed inspections. Good inspections should appear in adapter logs without corresponding rejection requests. Investigate `not_confirmed` or `unknown` using the logged inspection and command IDs. Do not claim success from dashboard count equality alone.

## Validation boundary

Tests exercise deterministic good/bad sampling, session/sequence identity, invalid/stale inputs, repeated observations, bounded deduplication, fresh counter baselines, missing/old/mismatched feedback and uncertain write results. An integration test reads the inspection over real loopback Modbus, passes it through conversion, ontology contextualization and SOSA serialization, makes the automatic decision, checks the deployed validator policy, performs a real Modbus rejection and reads device feedback. A second device request for the same bottle is rejected and the count remains one.

That integration test invokes pipeline handlers locally; it does not use real NATS/MQTT/AMQP brokers. Full broker-to-broker behaviour still needs the Kubernetes functional rehearsal after deployment. These checks do not constitute formal Paper 1 results.
