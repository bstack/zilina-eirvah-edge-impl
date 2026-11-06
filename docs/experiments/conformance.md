# E0: scoped architecture and execution checks

The offline evaluator implements selected checks from Study Plan v2.1 Definitions 1–8 and Conformance Matrix v1.0 C01–C13. It consumes the declared architecture and retained scenario evidence; it does not send commands, query secrets, change the cluster or modify the original evidence. No rebuild or redeployment is required.

## Run

Check the declared graph/configuration alone:

```powershell
uv run python scripts/conformance.py
```

Evaluate a saved scenario run that included `--collect-traces`:

```powershell
uv run python scripts/conformance.py --run results/scenarios/YOUR_RUN_DIRECTORY
```

For a new run, first use the scenario runner:

```powershell
uv run python scripts/scenario.py --context kind-eirvah-edge --repetitions 2 --collect-traces
```

Output is a new directory under `results/conformance/`, containing `report.json` and a readable `report.md`. Use `--output NEW_DIRECTORY` to choose the location. An existing directory is refused. Input paths and SHA-256 fingerprints, including the evaluator source, are retained alongside the evaluator Git revision and dirty-worktree flag.

Exit codes are **0 = all reported rule families pass**, **1 = at least one detected counterexample**, **2 = unknown remains without a detected failure**. Exit 2 is expected while the study still has evidence gaps; it is not a transport or deployment error. An invalid pinned reference stops evaluation because there is no trusted rule set to check against.

## Verdict semantics

- **Pass:** supplied evidence satisfies the named check within its stated scope and assumptions.
- **Fail:** sufficient supplied evidence demonstrates a contradiction, with offending edge/route or record/command IDs retained.
- **Unknown:** evidence, binding, ordering or clock bounds are insufficient. Missing records do not establish that an operation never happened.

Whether an experiment was **not run** is recorded separately in the `execution` field. A rule family aggregates its subchecks: a failure takes precedence; otherwise an unknown remains unknown. For example, C02 can pass its declared-route check while its runtime-boundary protection remains unknown. C01 passing does not make the deployed system conformant.

The evaluator always reports `universal_conformance=not_established`. Finite observations cannot prove the universal trace inclusion of Definition 7. It trusts the supplied records and configurations as evidence inputs; it does not independently attest signatures, firmware, deployed images or physical effects.

## Check coverage

| Matrix rule | Executable checks in this version | Evidence still needed for the complete rule |
| --- | --- | --- |
| C01 / Definitions 1, 2, 4, 7 | Nonempty typed graph, unique IDs, declared roles, permitted triples, edge/contract kinds, channel/component references and source bindings | Runtime realization is a separate claim |
| C02 / Definitions 3–5, 7 | Every pinned required route exists; removing namespace/executor-role vertices cannot leave a bypass path; deleting a requirement cannot silently weaken the check | Isolated unapproved-writer refusal with independent native/device evidence |
| C03 / Definitions 3–5 | Selected five-control topics have valid syntax, declared command version, admission policy and native/OPC UA mappings | Full industrial identity/description semantics and supported receiver versions across every flow |
| C04 / Definitions 3, 5 | Explicitly not run when equivalent native inputs/outputs are absent | Versioned golden dataset and semantic comparisons from E2 preparation |
| C05 / Definition 6 | Pinned consumer replay checks latest timestamps, equal-time conflicts, late/future/bad samples, stale/missing state and fresh consumer recreation on three consumed streams | State has no source epoch/sequence; real publisher/pod restarts and complete deployed-stream coverage remain separate |
| C06 / Definition 6 | Consumer replay checks immutable retained inspection IDs, duplicate/bottle suppression, expiry, capacity/retention, sequence gaps/regression and source-session changes. Existing command traces separately check bottle-specific counter/identity feedback | Real delivery/effect accounting and restart recovery remain unestablished by component replay; snapshots are not a durable event log |
| C07 / Definitions 5–6 | Observed writer and target policy; same-process admission for the same attempt and immutable fields; matching prior validator approval under trusted-record assumptions | Bounded cross-component ordering, full authority-binding and negative writer cases; logs alone do not cryptographically attest approval |
| C08 / Definition 6 | Initiation time interval compared with the command deadline | A justified bound on UTC clock uncertainty at the execution boundary |
| C09 / Definition 6 | Recorded binding consistency and duplicate initiation detection within the declared window; identical collected records are deduplicated by record ID | Complete history and selected ledger/restart/retention boundary cases |
| C10 / Definition 6 | Typed reported outcomes and acknowledged-completion timestamps; known write uncertainty cannot be labelled a definite refusal | Full fault/device reconciliation is explicitly not run without case evidence; the whole rule remains unknown |
| C11 / Definitions 3, 5–6 | Explicitly not run without loss/mixed-traffic traces | Broker loss, reconnect, congestion, declared delivery/ordering policies and mixed workloads |
| C12 / Definitions 6–7 | Record integrity, contradictory copies, named missing stages, per-action counts and generated/observed command-ID reconciliation | Complete capture across gaps/restarts, actual build/configuration attribution and clock policy |
| C13 / Definition 8 | Every deployed component maps to a nonempty declared host set | Runtime placement/build validation and agreed distributed deployment demonstrations |

The deferred experiments are not replaced by the checker. They are inputs to be developed in the subsequent failure, interoperability, scaling and deployment work. Their absence is visible rather than interpreted as success. Existing unit and local cluster tests remain engineering evidence; this version does not import a historical `pass` summary as proof of a full rule.

## Reference and evidence boundaries

`config/architecture/conformance-reference.json` pins the selected profile and required routes independently from the submitted manifest. It records the study/matrix document names and fingerprints. `--manifest PATH` can evaluate another realization; `--reference PATH` deliberately selects another explicit reference. Editing only the realization cannot authorize a new role/flow triple or remove a required route. Reference format version, architecture format version and individual message contract versions are separate concepts.

Static configuration checks inspect the repository's declared bindings, not whatever configuration may currently be running. The original manifest's `conformance_verdict` remains unchanged. The report is the scoped evaluation artifact, not a permanent certification written into the architecture model.

Execution checks use the records from [execution records](execution-records.md):

- Monotonic ordering is used within a component/process instance or a verified matching Linux clock domain. Otherwise temporal ordering uses UTC intervals with finite, nonnegative uncertainty bounds; a verified approval ticket can separately establish causal issuance-before-admission. Null, invalid or infinite bounds do not become zero.
- For C08, the whole initiation interval before or on the deadline supports pass; the whole interval after expiry is a violation; overlap or absent bounds is unknown. Completion time is never substituted for initiation time.
- Admission requires an attributable approval identity and a matching writer/attempt. A missing admission is unknown, not automatic proof of bypass. Known later refusal of the specific selected approval is a counterexample; ambiguous cross-component ordering stays unknown.
- C09 counts application initiation records, not the multiple native frames used by one operation. Rejected conflicting requests do not count as admitted mutations. Two initiations provably within the declared window fail; when the relationship or expired-ledger state is uncertain, the result stays unknown.
- Unknown command outcomes are not themselves failures. They can be correct recovery behaviour. Native acknowledgement is distinct from observed process response; simulator feedback is not a measured real plant.
- Record IDs copied across current/previous logs are counted once. Conflicting copies are an evidence-integrity failure. Retained logs cannot establish that no other records were lost.

The current logger deliberately emits `clock_uncertainty_s=null`. Consequently the real run's absolute deadline and cross-component admission ordering checks remain unknown. The evaluator does not assign an invented clock tolerance to make them pass.

## Deliberate violation checks

Run the maintained checker regressions:

```powershell
uv run python -m pytest tests/scripts/test_conformance.py -q
```

Cases cover missing endpoints, undeclared roles, contract-kind/version mismatches, profile drift, bypasses, missing routes, missing policy data, invalid observed writes, bounded-clock expiry, ambiguous clock intervals, absent admission, admitted payload mutation, duplicate initiation, retention limits, uncertain outcomes mislabelled as refusal, dropped records and conflicting record copies. A deliberate violation is successful as a test when the evaluator reports the expected failure. Synthetic fixture clock bounds are test inputs, not measured bounds for the live cluster.

## Local validation — 4 November 2026

The full regression suite passed with **445 tests and 16 skips**, including **43 evaluator cases**. Strict mypy, Ruff and diff checks passed. Deliberate endpoint/route, policy, expiry, identity, duplicate-initiation and outcome errors were detected; missing and ambiguous evidence stayed unknown. The actual CLI was also checked with a retained scenario package, a deliberately broken manifest and malformed JSON.

For the retained two-iteration scenario, C01 passed and the overall verdict was **unknown**: one rule family passed and twelve remained unknown. Positive subchecks included required routes, selected command bindings, observed policy checks, binding/initiation consistency where records existed, outcome representation, record integrity, visible-stage coverage and generated/observed ID reconciliation. Whole-family verdicts preserve the remaining runtime boundary, timing, completeness and experiment obligations.

Readable local report: `results/conformance/2026-11-04T23-00-47-888816Z/report.md`; detailed evidence and fingerprints are in `report.json`. The source run is `results/scenarios/2026-11-04T22-29-09-224589Z`. These paths are gitignored engineering artifacts. Evaluation was offline and did not alter the cluster or the retained run.

### Follow-up: distinguish false unknowns from missing experiments

The warmer policy omits `value_type` because `NodePolicy` declares `double` as its default. The evaluator now applies that same declared default rather than treating those records as incomplete. Both the valid default and an incorrect declared type have regression coverage; all 45 evaluator cases pass. The corrected report is `results/conformance/2026-11-04T23-05-47-465330Z/report.json`.

The remaining overall unknown is not evidence of a failed functional run. The report aggregates the full selected study obligations, including semantic/state/event, fault/mixed-traffic and distributed-placement experiments that this control scenario did not perform. Actual timing/capture gaps require new measurements; the saved records cannot retrospectively establish a clock bound or recover missing actions. Local command-case results and whole-study evidence coverage should remain separately scoped when the next E0 experiment package is defined.


## Bounded command capture and deployment transfer

`scripts/e0_capture.py` collects the prospective case in `config/architecture/local-e0-case.json`. It exercises five controls, exact replay, an out-of-range refusal, an already-expired fresh request and a direct writer request without approval. It pauses the automatic controller for isolation and restores its previous replica count in `finally`. It keeps native simulator feedback and read-only writer-ledger evidence separate from reported acknowledgements.

```powershell
uv run python scripts/e0_capture.py --context kind-eirvah-edge --repetitions 1
```

Private, bounded process journals are read through authenticated `kubectl exec`; no evidence HTTP endpoint is exposed. Start/end sequence fences, process identity, pod/image identity, selected source/configuration hashes and the ledger volume identity are retained. Restarts, missing sequences or journal capacity overflow prevent a complete-capture verdict. The journal is temporary pod-local evidence, not an archive across pod replacement; export it before replacement. Its current capacity is 4096 records per process, intended for bounded engineering cases.

Raw records retain unknown clock uncertainty. Derived copies use before/after causal clock probes, timestamp resolution and the capture client's observed wall/monotonic relationship. The recorded model assumes no unobserved excursion outside that envelope during the bounded run; it does not establish external UTC accuracy. Shared Linux monotonic clocks are used only when kernel boot identity and time namespace match. Different machines are not assumed to share a clock. Matching verified approval fingerprints provide causal approval-before-admission evidence without comparing cloud and edge timestamps. Deadline checks still require a sufficient time bound and remain unknown if the interval overlaps expiry.

The local-case report evaluates only its pinned checks. The whole-study report retains unperformed state, event, fault, interoperability, scaling and deployment obligations as unknown. A passing local case is neither full architectural conformance nor proof of physical plant safety.

### Private, public and hybrid deployments

Collection is independent of provider-specific APIs. `--targets PATH` selects contexts and namespaces per component; an omitted component inherits the default. Broker service names and ports can also be overridden. Copy `config/architecture/deployment-targets.example.json` and replace its placeholders with explicitly selected research contexts. The same four command-service images and evidence format apply; native protocol reads run on the capture host through loopback port-forwards to each simulator's configured location, and the ledger is read at the writer's location. Clients are warmed before creating short-lived commands, so repeated container imports do not consume their deadlines.

```powershell
uv run python scripts/e0_capture.py --targets PATH_TO_YOUR_TARGET_MAPPING.json
```

This supports Kubernetes-based research deployments with Linux service containers and suitable authenticated kubectl permissions. It does not provision cloud networking or make a single-cluster service name reachable across clusters: deployed broker URLs, routes, authentication, secret distribution and persistent storage must be configured for the actual topology. The edge writer remains the sole authoritative writer for its ledger; moving it requires preserving the declared recovery/retention policy. The example mapping describes collection locations, not a deployment manifest or a completed hybrid experiment.

Prometheus remains useful for rates, latency histograms, resource use and scrape coverage. Its aggregate series supplement command journals; they cannot replace command identity, verified approval binding, native feedback or missing-record checks. Use the same metric names and bounded labels across deployments; avoid command IDs as metric labels. Retain the chosen monitoring topology and scrape/export configuration in each experiment package.

The four command deployments use a bounded startup probe before normal readiness/liveness checks. Local image startup under CPU limits exceeded the previous liveness grace period; runtime probe/resource policies are retained with the pod inventory. This prevents startup restarts from being treated as a valid evidence window and does not relax normal post-start liveness checks.


## Bounded capture validation — 5 November 2026

Two completed captures passed the pinned 21 local command-case checks. The first retained 15 submissions and 110 correlated records; the repeat with two scenario iterations retained 29 submissions and 212 records. Both had no detected sequence/build/configuration/clock-bracket capture errors and confirmed restoration of the automatic controller. Conditional healthy-reset skips explain the submission counts. Each capture also tested a separate unsigned writer request.

- First package: `results/e0-capture/2026-11-05T08-30-24-093670Z/`.
- Repeat package: `results/e0-capture/2026-11-05T08-32-06-277107Z/`.

Each contains raw start/end capture, inventory, scenario journal, native read-back snapshots, read-only ledger export, clock calibration, controller restoration state, local-case report and whole-study report. The whole-study verdict remains unknown. Earlier stopped attempts were retained separately and were not reclassified as successful; no command was blindly resent to turn an unknown into a pass.

The regression suite passed with 460 tests and 16 skips. Strict mypy checks passed for capture/scenario/conformance and the changed contracts, observability and service code. Provider-routing tests cover distinct component contexts/namespaces and native transport/ledger targeting; actual public, private and hybrid runs have not yet been performed. Capture journal I/O and measurement overhead must be included when preparing the later scalability experiments.


## State and event checks — C05/C06

Run the production consumer code against the prospective finite case plan, without connecting to a broker or cluster:

```powershell
uv run python scripts/state_event_case.py
```

The runner creates a new `results/state-event-e0/<timestamp>/` package with `evidence.json`, `report.json`, `report.md` and frozen copies of the relevant source, policy and case files. Existing output directories are refused. Inputs, injected evaluation times, actual consumer-instance identities, decisions, latest-state snapshots and generated command objects are retained. The commands are never published or written to a device. The small inspection capacity of two is an isolated stress-case setting; the deployed default remains 4096.

Include that evidence in the whole-study evaluator, optionally alongside a saved command run:

```powershell
uv run python scripts/conformance.py --data-evidence results/state-event-e0/YOUR_RUN/evidence.json
# To combine with existing command evidence, also supply --run PATH_TO_COMMAND_RUN
```

`config/architecture/state-event-policy.json` pins the selected policy and `config/scenarios/state-events.json` pins 31 cases and their expected outcomes. The offline checker does not import the runtime implementation to compute its expected answers: it compares retained decisions to this independent case oracle. Changed inputs, contradictory snapshots, unexpected commands, missing sequence diagnostics, reused command IDs and unrecorded consumer-instance changes are checked. Missing evidence stays unknown. An expected refusal of an injected invalid input is a passing consumer test, not a claim that the invalid input conforms to its contract.

### State policy and its boundary

The selected streams are conveyor speed setpoint, rejection count and last rejected bottle, as consumed by the decision service. Valid good-quality samples can update a stream only with a strictly newer source timestamp. Equal-time identical values are duplicates; equal-time changed values are conflicts. Neither can overwrite the current value. Future, stale, out-of-order or bad-quality samples are ignored. State age is checked against the five-second profile: snapshots become stale when updates are missing and start unknown after a consumer is recreated. A stale snapshot retains its last known value with an explicit stale status; it is not a fresh observation.

Existing SOSA state messages contain no publisher sequence or epoch. The implementation therefore applies this narrower timestamp policy; it cannot prove order across a publisher clock reset or authenticate a new source epoch. The publisher-restart case is an input marker followed by old/new timestamp samples, not a measured restart of a deployed publisher. The snapshot accessor is available to the runner; this change does not claim a new live Grafana freshness alarm or monitoring endpoint.

### Inspection policy and its boundary

An inspection's session/sequence ID binds its retained bottle ID, result and inspection time. A repeated retained event or a second inspection for the same retained session/bottle does not generate another command. A changed retained identity is refused explicitly. The observed sequence frontier advances within a retained source session. A fresh, unique event with an older/equal inspection time may still be processed when it arrives late: events do not inherit latest-state overwrite rules. A backwards sequence paired with a newer inspection time is refused as inconsistent source progression; wrap must use a new session. Late unique events are diagnosed separately and cannot move the frontier backwards. Source progress is tracked separately from command admission so an observed-but-refused delivery is not counted as a missing snapshot. A gap emits a missing-sequence diagnostic; it does not establish how many physical bottles were affected.

Freshness is strictly less than two seconds in this profile; deduplication entries are retained for the existing age-plus-ten-second interval. Capacity exhaustion refuses work without evicting live identities. Cases check the boundary and subsequent capacity recovery. Recreating a consumer clears its in-memory state: replay of a still-fresh inspection can create a new command ID. This is explicitly captured as the current limitation, not presented as durable deduplication or at-most-one physical effect across restarts. A changed source session is exercised as an input-identity case, not proof of a device restart or an ordering relation between opaque sessions.

The consumer report can pass while C05/C06 whole-family verdicts remain unknown. Broker replay/reconnect, real consumer/device restarts, complete sampled-event coverage and independent command/acknowledgement/rejection-counter reconciliation belong to the following recovery experiments. The new evidence is repeatable component-level conformance evidence, not a performance measurement, distributed timing result or universal proof.

### Consumer changes and verification

The state checks exposed an equal-timestamp conflict that could select two production modes. The consumer now refuses that conflict. Inspection checks add explicit diagnostics for identity mutation, sequence regression, gaps and bounded capacity; default retention and memory-only restart behaviour remain explicit. These service changes require rebuilding/redeploying `decision-agent-stub`; the offline replay/evaluator tools do not require a deployment. No new public message fields, dependency, listener or provider-specific API was introduced.


### Verification — 5 November 2026

The final retained package is `results/state-event-e0/2026-11-05T17-09-51-826752Z/`: 30 cases and 70 input/restart/snapshot steps passed their consumer checks. Its `combined-report/` includes the earlier two-iteration command package and correctly retains the whole-study unknown verdict. The intentionally corrupted `checker-counterexample.json` and `deliberate-violation-report/` demonstrate detection of a stale-state acceptance violation; they are labelled checker fixtures, not real observations.

The full regression suite passed with 474 tests and 16 skips. Strict mypy passed for the six touched production/evaluator modules; Ruff and diff checks passed. Missing early observations were specifically tested to remain unknown rather than creating false later failures. Exact topic binding, source fingerprints, incomplete cases, duplicate evidence, stale-state contradictions and namespace changes have regression coverage.

The updated `decision-agent-stub` image was built and deployed only to `kind-eirvah-edge/eirvah-edge` using its existing Recreate policy. Live source fingerprints matched the checkout and the service emitted its ready event; all 21 pods were Ready. This is a deployment smoke check, not a live execution of the 31 injected cases or proof of physical restart recovery. No commands were sent by the new replay runner. Changes remain uncommitted until requested.


### Verification of the state/event extension — 5 November 2026

The final retained package is `results/state-event-e0/2026-11-05T17-16-00-308555Z/`: 31 cases and 73 input/restart/snapshot steps passed their consumer checks. `combined-report/` includes the previous command package and correctly retains the whole-study unknown verdict. The deliberately corrupted `checker-counterexample.json` and `deliberate-violation-report/` demonstrate stale-state violation detection; they are checker fixtures, not runtime observations.

The full regression suite passed with 475 tests and 16 skips. Strict mypy passed for the six touched production/evaluator modules; Ruff and diff checks passed. Missing initial observations remain unknown rather than producing a false later failure. Regressions also cover state conflicts, fresh unique late events, exact topic/command binding, source attribution, incomplete cases, duplicate evidence and sequence/capacity diagnostics.

Only the decision consumer was rebuilt and redeployed to `kind-eirvah-edge/eirvah-edge` using its existing Recreate policy. Live source fingerprints matched the final checkout; all 21 pods were Ready. This is a deployment smoke check, not a live execution of the 31 injected cases or proof of physical restart recovery. The new replay runner sends no commands to brokers or devices.


## Persistent observation identity and retained inspection recovery

The deployed recovery profile extends the legacy memory-only consumer cases above. Each protocol adapter records a stable producer identity, a persistent connection epoch and per-node observation sequence on its own PVC. Conversion, contextualization and MQTT publication preserve this position. The boundary is the **adapter observation**, not an invented PLC sample sequence. The consumer retains accepted positions and rejects previous epochs after restart. A publisher restart does not create a source epoch. MQTT publisher client IDs include the pod hostname to avoid overlapping restart clients disconnecting one another.

The Modbus simulator now commits an immutable inspection and its simulated process state before exposing it. HR60�69 supplies the oldest retained event; HR32 remains the legacy latest snapshot. The adapter republishes the FIFO head until the consumer commits durable ownership. A separately provisioned HMAC receipt binds that exact event ID and digest; the adapter verifies it before acknowledging the native head through HR70�74. Receipt fields alone are insufficient. Both HMAC holders, native access and Kubernetes administrators remain trusted; this does not authenticate arbitrary MQTT state publishers.

Consumer ownership and the original command ID, target, payload and deadline are committed together. Restart/replay reuses that immutable command; the existing authoritative writer ledger prevents a completed command from starting again within retention. Unknown native outcomes are not retried as new commands. Expired inspections remain explicitly recorded and receipted without starting a late rejection. Thus event delivery coverage is separate from timely successful process actuation.

The source retains pending events on `modbus-source-journal`; the consumer retains inbox/outbox/high-water state on `decision-recovery`. Adapter positions use their named observation-position PVCs. SQLite uses WAL and synchronous FULL. Services remain single authorities with Recreate placement. Source generation pauses at 4096 pending events rather than overwriting them; consumer command ownership refuses capacity above 128 pending requests. Protection assumes retained, SQLite-compatible storage and the declared 24-hour retention. Independent active replicas, PVC loss and exactly-once real-plant effect are not claimed.

### Bounded live recovery case

```powershell
uv run python scripts/configure_actuation_security.py --context kind-eirvah-edge
uv run python scripts/recovery_case.py --context kind-eirvah-edge
```

Ensure `actuation-security` has `inspection-receipt-key` before deploying the upgraded adapter and consumer. The configuration helper adds the missing key without rotating existing ingress/approval keys. Rebuild the three adapters, Modbus simulator, decision consumer and four state pipeline images together because the shared position metadata traverses the pipeline. Apply the base configuration and import the rebuilt `:local` images into the selected kind cluster before restarting those deployments. Do not delete existing command-ledger or recovery volumes.

The runner requires one active consumer and broker and explicitly selected research targets. It controls only the conveyor and named research deployments (including temporary command-ingress suspension), retains volumes, temporarily disables random simulator jams, and restores desired conveyor controls, jam configuration and replica counts. It performs real adapter, publisher, consumer and source pod replacements, a broker outage, old-epoch injection, replay of a completed inspection, interruption of an owned pending command while command ingress is paused, and a forged receipt. Source-generated IDs are reconciled with durable ownership, immutable writer records and independently committed simulated rejection-counter effects. Native acknowledgements are not presented as measurements from a real plant.

Each new output directory retains raw database snapshots, clock domains, pod/PVC identities, source hashes, non-secret source snapshots, commands, restoration status and an independently evaluated report. Collection uses authenticated `kubectl exec`, with no evidence HTTP listener. Five live checks supplement the 31 legacy replay cases. Missing records, a different clock domain without measured bounds, or incomplete restoration prevent a passing recovery verdict. These finite local checks do not make E1 scalability, E2 interoperability, E3 distributed placement or the entire study pass.

The same `--targets` component mapping as the bounded command collector supports collection across Kubernetes-based private, public and hybrid deployments. Actual networking, secure key provisioning and storage topology must be configured there. Shared local clocks are not assumed across machines. Different clock domains require a later measured-clock extension; the current runner keeps those conclusions unknown.

During local bring-up, the publisher's 100m and integration orchestrator's 200m CPU ceilings caused stale observations. Both baseline limits were increased to 500m after observing the saturation. This is an engineering prerequisite for the recovery rehearsal, not a scalability experiment or evidence for an HPA claim.

To combine the retained consumer and live recovery evidence with a command capture:

```powershell
uv run python scripts/conformance.py --run PATH_TO_COMMAND_CAPTURE --data-evidence PATH_TO_LIVE_RUN/consumer-replay/evidence.json --recovery-evidence PATH_TO_LIVE_RUN/evidence.json
```

The five legacy limitation entries are replaced only by their independently evaluated live equivalents. Omitted or malformed recovery evidence cannot silently create a pass. Other unmet study obligations remain visible.


### Live recovery verification � 6 November 2026

The expanded run `results/live-recovery/2026-11-06T16-31-26-189840Z/` passed all five bounded live checks. Its 118 source-created inspections reconciled with 118 durable consumer records: 49 passed inspections, 8 completed rejections, 56 expired inspections and 5 refused commands. The eight completed rejections matched eight independently committed simulated native counter increments. No pending or unknown commands remained in the captured window. Expired/refused work was preserved explicitly; delivery coverage is not a claim that every failed bottle was rejected during outages.

Adapter and publisher restarts, a source restart with retained counters/events, consumer restart, replay, broker outage and forged receipt were exercised. A separate pending inspection command was owned before command ingress was paused and the consumer restarted; recovery retained the original ID, payload and deadline and refused the expired request. Shared Linux clock-domain evidence was recorded for this local case. Original replica counts, simulator jam configuration and desired conveyor controls were restored, and all 21 pods were Ready afterwards.

`report.md` contains the five live verdicts; `combined-report/report.md` combines these records with the 31 consumer cases and the previous bounded command capture. **C05 and C06 pass within this selected profile.** The full study remains unknown (three rule families pass, ten remain unknown), with no failed rule family in that combined report. Unperformed interoperability, mixed-flow/fault, distributed-placement and other evidence obligations remain separate. Earlier partial captures are retained and have not been relabelled as successful.

Final validation: **500 tests passed, 16 skipped**; strict mypy passed for 19 selected runtime/evaluator modules; changed Python files passed Ruff; `git diff --check` passed; the base deployment passed Kubernetes server-side dry-run validation. These are local engineering results, not controlled scaling measurements or real-plant validation.


## Bounded command fault cases

`scripts/fault_case.py` exercises the prospective profile in `config/architecture/command-fault-case.json` through the authenticated AMQP command path. The selected physical boundary is the Modbus simulated rejection mailbox. This extends E0 outcome/recovery evidence; it does not establish equivalent fault behaviour for OPC UA/S7, mixed-flow congestion or distributed performance.

| Case | Required observation |
| --- | --- |
| Caller reply subscription lost | Writer completes once; exact replay returns its original completion and causes no further effect. |
| Disconnect before the first write | Writer eventually records a retryable pre-write failure with zero effects. The caller may initially time out; exact replay can complete once after that history is established. |
| Sequence commit dropped | Payload staging has begun, so the writer reports unknown. Independent source evidence shows zero committed effects; replay must not start another attempt. |
| Native success acknowledgement lost | Source records one effect while writer/caller report unknown. Exact replay remains unknown and causes no additional effect. |
| Writer restart after effect, before acknowledgement | Real pod replacement retains the ledger PVC and interrupted identity. Replay remains unknown; independent source evidence shows one effect, not two. |
| Valid command delayed past expiry | A still-valid command is published while ingress is paused. After expiry and resumed ingress it is refused without entering native transport. |

The distinction between zero-effect and one-effect unknown cases is deliberate. `unknown` is an honest application outcome, not a failed test and not proof that no effect occurred. Administrative inspection of simulated effect records provides experiment evidence; it does not automatically resolve that uncertainty in the application or claim an exactly-once physical guarantee.

### Execution and isolation

```powershell
uv run python scripts/fault_case.py --context kind-eirvah-edge
```

The runner pauses automatic decisions, stops the simulated conveyor and selects six distinct available bottle IDs. It temporarily adds a sidecar to the existing single writer pod, using the same image and ledger PVC. A private ConfigMap redirects only the rejection target through a mailbox-restricted TCP proxy listening on **127.0.0.1 inside that pod**. There is no proxy Service, ingress, public evidence API or new application fault switch. Plans and bounded evidence files are read/written through authenticated `kubectl exec`. Simulator/device access, administrative access and the existing broker trust boundary remain declared assumptions.

Normal device timeouts are retained for the ordinary interruption cases. Only the restart barrier temporarily extends the selected target timeout to let the experiment interrupt an in-flight write after a successful device acknowledgement has been withheld. That setup change itself restarts the writer before the interruption case. These timings coordinate fault placement; they are not performance measurements. Every case waits for the prior native attempt to settle before changing its fault plan, even if the caller has already timed out.

Cases retain the full original command for replay, source snapshots and effect serials, private proxy decisions, bounded writer journal snapshots, deployed-source hashes, writer ledger rows, clocks and pod/PVC identities. The pre-restart snapshot is a complete journal prefix at its recorded fence, not a claim that the terminated process tail was exported. The independent evaluator checks source counter deltas, exact native values/sequences, approval-bound initiation counts, immutable command digests and recovery outcomes. Missing/overflowed journals, changed writer code, inconsistent records, unresolved clocks or incomplete restoration cannot silently pass. Checks use explicit conditions and also remain active with Python optimization enabled.

`finally` restores the original writer pod template before removing the temporary ConfigMap, restores the original conveyor controls/fault state and original consumer/ingress replica counts. It never deletes recovery volumes or rewrites old command identities. Each run has a new evidence directory; partial and failed runs remain retained. A restoration failure is recorded explicitly and prevents a passing capture.

```powershell
uv run python scripts/conformance.py --run PATH_TO_COMMAND_CAPTURE --data-evidence PATH_TO_CONSUMER/evidence.json --recovery-evidence PATH_TO_RECOVERY/evidence.json --fault-evidence PATH_TO_FAULT_RUN/evidence.json
```

The fault evidence supplements C10 outcome checks. C11 mixed-traffic/congestion experiments and the broader study obligations remain separate. Passing these finite cases is not universal conformance, safety certification or a real-plant result.


### Command-fault verification � 6 November 2026

`results/command-faults/2026-11-06T18-28-37-532757Z/` passed all six prospective fault cases. Exact replay produced one effect for the lost caller reply and pre-write disconnect cases; retained unknown for the dropped commit (zero effects), lost native acknowledgement (one effect) and writer restart (one effect); and refusal without native initiation for the delayed expired request. The delayed request was confirmed published before its deadline and held beyond it. The successful capture used a two-second request budget; the runner now uses ten seconds and a twelve-second hold to provide more setup margin without changing the predicate. This is fault placement, not a latency result.

Four native effects in total matched four source counter increments, with no duplicate initiation in the retained command records. Real writer pod replacement preserved the ledger volume. Restoration was confirmed, the temporary proxy/ConfigMap were removed, and all 21 pods were Ready. Previous setup/sequence failures remain retained as incomplete or failed rehearsals rather than being rewritten.

The combined report, using a fresh consumer replay plus the retained command and recovery packages, has **C01, C05, C06 and C10 passing within the selected profile**, nine remaining rule families unknown and no failing rule family. The full study remains unknown. Mixed-flow congestion (C11), interoperability and distributed evaluation are still separate obligations; neither the source effect audit nor the pre-restart journal prefix establishes a real-plant exactly-once guarantee.

Final checks passed: **525 tests, 16 skipped**, strict mypy for the four new/updated evaluator-runner modules, Ruff and `git diff --check`. No production service source change or cluster rebuild was needed; the live experiment used temporary configuration around the existing writer image.


## Standalone E0 assurance: contracts, admission and evidence coverage

The selected local profile adds explicit checks for C02 (runtime bypass), C03 (contract compatibility and meaning), C07 (admission), C08 (expiry), C09 (identity and retained history) and C12 (capture coverage). These are bounded E0 checks, not universal conformance.

`config/architecture/contract-catalogue.json` binds contracts to versions, flow kinds, sources and receivers. Internal envelopes emit version 1.0 and reject explicitly unsupported versions; an omitted internal version retains the documented v1 compatibility default. Public SOSA observations require explicit version and value type. Native mappings have catalogue versions without inventing version fields in PLC registers.

Before acting, the decision receiver validates namespace topic, sensor, feature, property, unit, value type, timestamp and inspection meaning against its deployed ontology. The contextualizer also rejects normalized unit/type mismatches. This validates declared meaning, not arbitrary MQTT publisher authenticity. Administrators, native-device access and provisioned HMAC-key holders remain trusted.

### Running the selected profile

```powershell
uv run python scripts/e0_capture.py --context kind-eirvah-edge --assurance
```

Rebuild the shared-contract consumers together: three protocol adapters, Modbus simulator, four state-pipeline workers, four command actors and decision consumer. Apply the decision observation-contract ConfigMap/mount. Preserve ledger/recovery PVCs. The converter CPU limit is now 500m after its previous 100m ceiling caused stale observations; this is engineering preparation, not an E1 result. The standalone contextualizer load generator now derives units and types from the ontology.

The capture exercises unsigned, tampered, wrong-writer, expired and envelope-mismatched direct writer requests. Evidence checks rejection, absence of admission/ledger/native initiation, and unchanged selected native feedback. Keys and operative approval signatures are never retained. Automatic decisions and random jams are temporarily paused/disabled and restored afterwards. Native snapshots select available test bottles while the durable inspection consumer is paused; this is setup, not E2 interoperability evidence.

C12 checks actor-journal sequence fences, raw/calibrated record correspondence, clock bounds, exact submitted/observed command IDs, pod/image/config/PVC identities, deployed source hashes and restoration. Incomplete or unrelated evidence cannot close another run's unknowns. Complete no-write evidence allows a verified refusal to satisfy an execution-only admission/deadline implication without claiming a write occurred.

Production SQLite ledger checks use an isolated results directory and injected times before, exactly at and after the declared 24-hour retention boundary, plus preparing/started/completed recovery and stale-token fencing. This is an accelerated component check combined with bounded live evidence, **not a 24-hour soak or complete lifetime history**. Live identities and recovery volumes are preserved.

```powershell
uv run python scripts/conformance.py --run PATH_TO_CAPTURE/run --assurance-evidence PATH_TO_CAPTURE/assurance-evidence.json --data-evidence PATH_TO_RECOVERY/consumer-replay/evidence.json --recovery-evidence PATH_TO_RECOVERY/evidence.json --fault-evidence PATH_TO_FAULT/evidence.json
```

Independent checkers evaluate retained evidence rather than trusting runner success. C04 interoperability, C11 mixed-flow/congestion and C13 distributed placement remain separate obligations.


### Standalone assurance verification - 6 November 2026

The bounded capture `results/e0-capture/2026-11-06T20-28-20-218466Z/` passed its local case with 102 execution records and 14 submissions, including five direct-writer negative cases. Its contract, bypass, retention and capture-coverage checks pass within the selected profile. The new public-wire contracts were also exercised in fresh recovery and fault reruns: `results/live-recovery/2026-11-06T20-34-40-215679Z/` passed all five recovery checks; `results/command-faults/2026-11-06T20-41-07-566450Z/` passed all six fault checks.

The combined report at `results/conformance/2026-11-06-final-standalone-assurance/report.md` records ten passing rule families, three unknown and no failed family. C02, C03, C07, C08, C09 and C12 now pass in this bounded local profile. C04, C11 and C13 remain unknown because interoperability, mixed-flow/congestion and distributed-placement experiments have not been supplied. The study-wide verdict therefore remains unknown. Historical partial captures are retained unchanged.

Validation: 563 tests passed and 16 skipped; strict mypy passed for 16 changed/related modules; changed Python files passed Ruff; `git diff --check` and Kubernetes server-side deployment dry run passed. Recovery/fault restoration was confirmed and all 21 pods were Ready. The retention boundary was checked with injected time, not a 24-hour live soak. Code remains uncommitted until requested.
