# Repeatable functional scenario runner

Run from the repository root. This is a sequential functional rehearsal, not a fixed-rate load generator or the E0 conformance checker. It uses the existing `experiment-driver` AMQP identity, validator, writer and command ledger. No service images or cluster rebuild are required.

## Run

Validate and display the versioned plan without connecting to the cluster:

```powershell
uv run python scripts/scenario.py --plan
```

Run two repetitions and collect the correlated service records:

```powershell
uv run python scripts/scenario.py --context kind-eirvah-edge --repetitions 2 --collect-traces
```

The runner opens its own loopback RabbitMQ and Mosquitto port forwards using the explicit context and namespace (`eirvah-edge` by default). It reads only the experiment-driver AMQP URL from the existing `actuation-security` Secret into memory. URLs, credentials and raw transport exception messages are not written to reports. `kubectl` must have the corresponding read/port-forward access. MQTT uses the existing local demonstration credentials; override `EIRVAH_MQTT_USERNAME` and `EIRVAH_MQTT_PASSWORD` when those differ.

The Windows entry point uses a selector event loop for MQTT compatibility. Existing manual forwards need not be stopped: the runner selects free loopback ports and closes only its own processes afterwards.

## Included scenario

`config/scenarios/controls.json` defines `five-control-functional-v1`:

1. Set the warmer recipe to 25 C.
2. Read fresh conveyor fault feedback; reset through the command path only if faulted.
3. Stop the conveyor, set its speed to 0.4 m/s and start it.
4. Close and reopen the filler valve.
5. Read fresh capper fault feedback; reset only if faulted.
6. Stop the capper, set its speed to 1200 rpm and start it.
7. Reject a fresh passed-inspection bottle through the normal command path.
8. Replay the exact earlier capper-speed command, checking its replay flag and original completion timestamp.
9. Request 2000 rpm and require the expected policy-range refusal.
10. Select a final warmer recipe of 22 C.

There are 15 declared steps per repetition. Healthy-motor resets are explicitly reported as skipped, not as executed tests. The runner leaves the declared final settings selected: conveyor running at 0.4 m/s, capper running at 1200 rpm, filler open, warmer recipe 22 C, with observed motor faults cleared. It does not restore the original settings or counters. On failure, partial state remains explicit; inspect the evidence before continuing.

The manual rejector check deliberately selects a passed simulated bottle so it does not compete with the automatic failed-inspection rejection rule. It tests the rejection actuator, not classification accuracy. Selection requires good-quality UNS inspection data, a matching inspection timestamp no more than two seconds old, and an inspection identity not already selected in this run. Bottle identities vary and are recorded; hard-coded IDs are not used. A stopped conveyor, missing observations or another concurrent controller can prevent the scenario from completing. Stop other manual command/disturbance generators for a predictable rehearsal. The existing automatic supervisor/inspection agent may remain running; its commands can interleave and are not part of the runner's offered command count.

## Plans and timing

Use `--scenario PATH` for another JSON plan. Each step names a target, action, optional value and expected `approve`/`reject`. Optional `reason_contains` checks a policy refusal. `replay_of` refers to an earlier unconditional successful step and cannot redefine its command. `value_source=passed_bottle` is only valid for rejection; `if_faulted=true` is only valid for conveyor/capper reset. Duplicate names, forward references and invalid operations are rejected before cluster access.

Each repetition creates new command IDs. Replay retains the entire original command, including request time, payload and deadline. `--interval` is a minimum delay after completed steps, default 0.25 seconds; connection setup, command round trips and observation waits also take time. It is not a promised arrival rate. Transport/result waiting is bounded to 20 seconds per submission; MQTT observation waiting is bounded to 15 seconds. No uncertain command is automatically retried.

## Evidence and result meanings

A new directory under `results/scenarios/` contains:

- `scenario.json`: the frozen, normalized plan. `scenario_sha256` fingerprints its exact UTF-8 bytes, including the final newline.
- `journal.jsonl`: each command and resolved observation saved and flushed before submission, followed by its reported outcome. Replay records share the original ID. Transport failures retain the exception type without credentials.
- `summary.json`: expectations, actual outcomes, dynamic inspection IDs, repetition numbers, UTC times, selected cluster, source revision and worktree-dirty flag.
- Optional `traces/COMMAND_ID/`: service execution records and their separate collection summaries.

The runner refuses an existing output directory; use `--output NEW_DIRECTORY` to choose a location. Ctrl+C retains an interrupted summary and marks an in-flight command uncertain. An abrupt kill can leave `outcome=running`; treat that as incomplete and inspect the journal. Do not blindly recreate a replacement command with a new ID.

A step passes only when the correlated command echo and expected decision match. Completion requires a timezone-aware writer completion time. Replay also requires `replayed=true` and the original completion timestamp. A pending, unknown, malformed or mismatched reply stops the run as unknown. An unexpected definite decision/rejection reason stops it as failed. Exit codes: 0 completed expected scenario, 1 definite scenario failure, 2 unknown outcome/setup/collection issue, 130 interrupted.

A scenario pass describes reported application outcomes, not independent proof of physical effects or absence of duplicate writes. Trace collection completeness and process feedback remain separate evidence; see [execution records](execution-records.md). Optional trace errors produce exit code 2 while preserving the scenario outcome and collection errors separately. All reports retain `formal_conformance=not_assessed`. Later E0 checks will assess architecture/execution rules; failure campaigns, concurrency, load patterns and controlled-infrastructure measurements remain separate roadmap steps.

## Local validation — 4 November 2026

The local regression suite passed with 402 tests and 16 skips; 21 new runner cases cover exact replay, stop-on-unknown behaviour, expected refusals, fresh inspection selection, repetition identity, interruption and evidence preservation. Ruff and strict mypy checks passed.

Two live iterations completed: 27 steps passed and three healthy-motor resets were explicitly skipped. All five controls, both policy refusals and both replay checks behaved as expected. Correlated traces were collected without reported collection errors; completeness remains unestablished. Final readback showed filler open, conveyor running at 0.4 m/s, and capper running without a fault at the requested/actual 1200 rpm. All 21 pods were Ready.

Local engineering evidence: `results/scenarios/2026-11-04T22-29-09-224589Z/summary.json` and its journal/traces (gitignored). That live package used the earlier normalized-JSON fingerprint convention; the final implementation and regression test fingerprint the exact preserved file bytes. No service image or cluster recreation was required.
