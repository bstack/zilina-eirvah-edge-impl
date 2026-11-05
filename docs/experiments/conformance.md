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
| C05 / Definition 6 | Explicitly not run for absent state sequences | Ordered/late/missing samples, restart epochs and declared freshness policy |
| C06 / Definition 6 | When present, bottle-specific process records require the requested bottle ID, increased counter and post-request observation times | Delivery/event/effect accounting, retention, restarts and sampled-event coverage |
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
