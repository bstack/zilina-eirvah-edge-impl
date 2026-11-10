# E2 interoperability preparation

This is a repeatable **in-process native-format replay**, not a deployed interoperability experiment. It prepares equivalent inputs and independent expected outputs for the three supported industrial protocols. C04 and the study verdict remain unknown until native acquisition, broker delivery and the remaining E2 tasks have been evaluated.

## Run

From the research checkout:

```powershell
uv run python scripts/interoperability.py
```

Each invocation creates a new `results/interoperability/<UTC timestamp>/` directory. It retains the native fixture inputs, original decoded values, normalized values, public observations, semantic command outputs, source hashes and source snapshots. The Markdown/JSON report distinguishes preparation checks from the whole-study verdict. Existing result directories are never overwritten. No cluster, broker or device is contacted; running this command does not change the local deployment.

## Selected comparison

`config/experiments/e2-golden.json` pins 17 synthetic samples across speed, temperature and a rejection counter. The native representations are authored separately: OPC UA typed values, Modbus integer words with explicit scale, and big-endian S7 REAL/INT bytes. They describe equivalent logical signals; the deployed stations are not silently reconfigured to expose the same sensor through all three protocols.

| Variant | Work performed |
| --- | --- |
| Canonical contracts | Production adapter helpers, the deployed conversion catalogue, production contextualizer, orchestrator publish-request construction, SOSA serialization/validation and the production decision receiver. |
| Source-specific consumer | Consumer-owned protocol decoding and local identity/unit/type mappings, followed by the same declared engineering precision and production-mode rule. No canonical message is required by that baseline; its local meaning is projected into common comparison fields only for the oracle. |

Each variant/protocol combination has independent rule state. Golden expected identities, units, types, numeric values, timestamps and command semantics are compared with each result, rather than merely comparing the two variants with one another. Dynamic correlation IDs and descriptive reason text are excluded from semantic command equality. Hysteresis, mode transitions and suppression of same-mode commands are checked. Temperature verifies information equivalence; it does not itself trigger the current speed-driven warmer rule. Counter representation is restricted to the common 0..32767 range for this initial comparison.

The 102 matrix cases include six expected over-range refusals. A numeric comparison tolerance of 1e-6 handles S7 REAL representation for information comparison; it does not relax command equality or controller thresholds. Actual public payloads are checked against the reported output meanings. Missing/duplicate cases, changed inputs, source/config attribution failures, changed meaning and changed decisions cannot produce a passing preparation report.

## Relation to the architectural model

The canonical variant exercises the existing integration/contextualization, UNS information and decision responsibilities; it adds no new logical node or bypass. Expected identities, units, types and the selected precision profile make the contract-meaning obligations concrete. Equal semantic commands provide bounded behavioural evidence for the selected controller, rather than proof over every possible trace. The source-specific variant satisfies the same engineering requirements through consumer-owned mappings. C04 remains unknown because this function replay alone does not establish deployed protocol interoperability.

## Conveyor speed engineering contract

The agreed **simulated conveyor profile** declares:

| Property | Value |
| --- | --- |
| Unit | m/s |
| Resolution | 0.01 |
| Valid engineering range | 0.00..0.80 |
| Rounding | Nearest grid point; decimal half-even at an exact tie |
| Bounded representation allowance | 0.00000003 m/s at the range check |

The allowance is less than one hundred-thousandth of the resolution step and approximately half a binary32 ULP at 0.8. It handles S7 REAL's representation of nominal 0.8 as approximately 0.800000012. It is **not** a half-step allowance for an out-of-range physical value. Range checking happens on the original engineering value, after scale/offset and **before rounding**. Values such as 0.800001, 0.801, 0.804 and 0.81 are rejected; rounding cannot turn 0.804 into an admitted 0.80. Non-finite and non-numeric values are rejected. Values within the bounded encoding allowance are clamped to the declared limit before quantization.

The raw envelope remains unchanged. The canonical converter applies the policy centrally; the source-specific baseline applies the same requirement within its consumer. This holds the engineering requirement constant while comparing integration responsibilities. It is not evidence that normalization is unique to UNS architectures or that every industrial device has this resolution.

The policy is explicit in both converter configuration files and the E2 profile. `simulated-conveyor-speed-v2` names the changed research profile; existing wire schema versions are unchanged. Earlier E0 captures remain evidence for their pinned code/config revisions and are not relabelled as tests of the new normalizer. The shared helper is `eirvah_contracts.measurement`; other signals retain existing pass-through behaviour unless they explicitly select a precision policy.

`precision-probe.json` retains a paired upper-bound demonstration. Feeding the unchanged decision rule the unrounded S7 value produces no command, whereas declared normalization produces the same 26 degree C recipe request as the other two representations. This is an in-process counterexample/correction, not a before/after performance measurement or a native device experiment.

## Deployment impact

The replay requires no redeployment. To use the new speed normalization in the running system, rebuild/import the data-converter image and apply its changed ConfigMap, then wait for its rollout. The converter owns the new policy; consumer rules and public wire fields have not changed. Preserve recovery/ledger volumes. A live E0 smoke capture should be repeated after that deployment before claiming current-cluster evidence for this revision.

## Remaining E2 work

The selected local preparation profile now covers native acquisition/publication, the gateway-alias source-addition task, selected quality/error policies and paired authenticated command/process response. See the 10 November verification below for the exact evidence boundaries.

1. Run controlled repetitions using equivalent workloads and declared acceptance budgets; reconcile results with E0 evidence.
2. Measure integration effort/errors prospectively if the paper claims reduced integration effort. Generated mapping changes alone do not measure human effort.
3. Include live automatic observation-to-command delivery in the controlled profile where that causal path is claimed. The paired operator replay below deliberately tests a different boundary.
4. Extend the declared case matrix when the final industrial scenario selects additional signals, event contracts or fault policies; this selected profile does not establish all possible protocol behaviour.
5. Analyse interoperability separately from E1 scaling and E3 public/private/hybrid deployment flexibility.

These fixtures are synthetic and tied to the selected simulated line. They are not presented as a published industrial dataset or a validated physical process model.


## Local verification - 7 November 2026

`results/interoperability/2026-11-07T05-56-29-062718Z/` retained a passing 102-case matrix (including six expected range refusals), two attribution/completeness checks and six passing paired upper-bound checks. The unrounded S7 value was 0.800000011920929; both normalized variants produced 0.8 and the expected warmer request. C04 remains unknown.

Final regression validation: **601 passed, 16 skipped**. Ruff, strict mypy for the three changed/new runtime modules and `git diff --check` passed. The existing feedback fixture was adjusted from an out-of-range 1 m/s to a valid 0.4 m/s; dedicated tests retain over-range rejection coverage. These checks used local production code in-process. The cluster was not redeployed and the changes have not been committed or pushed.


## Isolated native acquisition and broker rehearsal

```powershell
uv run python scripts/e2_native.py
```

This next stage exercises real OPC UA, Modbus TCP and S7 connections through the existing production subscriber runtimes. The production converter, contextualizer, orchestrator and MQTT publisher run as separate local Python processes. Separate disposable NATS/Mosquitto containers carry their actual request/reply and public MQTT traffic. A deterministic fixture container supplies equivalent speed, temperature and counter representations; it uses the native libraries from the existing `actuation-signal-publisher:local` image. Docker and that locally built image are prerequisites. There are no new dependencies.

Every published host port is bound to **127.0.0.1**. The test-only brokers carry synthetic data and use no application security keys; MQTT anonymous access is confined to this loopback rehearsal. No production broker, Kubernetes service, automatic line controller, command queue or actuator is used by this runner. Native fixture control uses a private mounted file, not an evidence HTTP endpoint. Readiness and connection settings are explicit. Worker subprocesses use the Windows-compatible Selector event loop.

Four cases per protocol select 0.4, 0.7, 0.8 and out-of-range 0.81 m/s, along with independently specified temperature and counter inputs. Production subscribers publish raw envelopes with persistent per-signal producer positions. An independent native client reads back the fixture values. The checker reconciles all 36 selected signal results by correlation ID with the captured NATS ingress and either the corresponding SOSA MQTT observation or the converter dead-letter record. It checks source endpoint, exact per-signal identity, native value, semantic binding, unit/type, source timestamp and position preservation. Both the canonical result and consumer-owned source-specific interpretation are compared with the golden expectations.

The existing production-mode rule is evaluated in-process for the resulting speed observations, with a fixed declared 100 ms observation age for both variants. Semantic commands must agree with one another and with the expected 22/26 degree C recipe transition, including no extra command when high mode continues. These are **derived decision checks**, not live consumer-to-AMQP command delivery, clock-synchronization evidence, latency measurements or independently observed actuator effects. Quality/error semantics beyond the selected converter refusal remain additional work.

Every run retains raw MQTT/NATS records, selected correlations, native readback, dependency/Python versions, adapter maps, worker settings/logs, Docker image IDs and loopback port bindings, process IDs/exit codes, source/config hashes and exact source snapshots. All subprocesses belong to that run. Container removal first verifies its unique owner label; failure to collect a log does not skip removal. Cleanup is independently confirmed and incomplete restoration prevents a passing verdict. Previous failed/incomplete runs remain unchanged.

`native_case=pass` covers only this selected native acquisition and broker profile. **C04 remains unknown**: source-addition effort, the remaining protocol/contract cases, full command delivery/device responses where selected and controlled comparative experiments still need their own evidence. The existing architecture-wide report is not silently promoted by a function or transport rehearsal.


### Native verification - 9 November 2026

`results/e2-native/2026-11-09T08-37-19-391372Z/` passed all 36 selected signal comparisons plus matrix completeness. Real production adapter loops, NATS request/reply and MQTT publication were exercised against isolated native fixture servers. Source-specific interpretations and derived recipe decisions matched the golden cases, including the 0.8 representation boundary and 0.81 converter refusal. Exact source snapshots and dependency versions were retained; no collection errors occurred and all owned containers/processes were cleaned up. This is the selected acquisition/broker rehearsal, not full C04 conformance or actuator-effect evidence.

The committed converter image `data-converter:5fbf81f` was deployed to `kind-eirvah-edge/eirvah-edge`; the live pod's exact precision policy was verified. Deployment evidence is in `results/converter-deployment/2026-11-07T10-23-03-371095Z/`. Applying the component alone encountered the full stack's immutable selector labels; a focused image/ConfigMap pod-template patch preserved that selector and completed successfully.

The 7 November live smoke capture (`results/e0-capture/2026-11-07T12-15-14-766286Z/`) passed the control scenarios, native feedback, replay and refusal checks, but its UTC deadline intervals remained unknown; its original controller replica count was restored. A later assurance capture (`results/e0-capture/2026-11-09T08-39-26-770595Z/`) was interrupted before journal collection and remains unknown. Its process was confirmed absent and its original one-replica automatic consumer was independently restored. This partial capture has not been relabelled as passing; a fresh assurance capture is still required for complete current-deployment verification.

Final tooling validation: **624 tests passed, 16 skipped**; Ruff, strict mypy for both native runner modules and `git diff --check` passed. Native runner changes are not yet committed/pushed. The next E2 work is the source-addition comparison and the remaining contract, quality and command/device-response cases, followed by controlled repetitions.


### Completed deployment-assurance rerun - 9 November 2026

The fresh capture `results/e0-capture/2026-11-09T09-42-56-217268Z/` **passed the bounded local case**, retaining 110 execution records and 15 submissions. C08 deadline checks now pass. Independent evaluation of `assurance-evidence.json` also passed runtime bypass, contract semantics, retained-history and collection-coverage checks. Its capture-only architecture report has C01, C02, C03, C07, C08, C09 and C12 passing. Other families remain unknown in that report because their separate recovery/fault/interoperability/deployment datasets were not supplied; this does not relabel earlier evidence or establish universal conformance.

The original automatic-consumer replica count and simulator jam binding were independently confirmed restored, and all 21 research pods were Ready. Docker initially could not start because of stale zero-byte runtime sockets. Only verified socket-only runtime folders were preserved as backups so Docker could recreate them; containers, volumes and the existing cluster were not reset. An early attempt while services were recovering remains retained as incomplete. The successful capture began after all pods were Ready.

This completed rerun supersedes the earlier pending deployment-assurance action. The remaining next actions are to commit/push the native E2 tooling, then complete the source-addition and remaining protocol/contract/command-response comparisons.


### Extended selected E2 preparation - 10 November 2026

The selected local preparation report **passes**: `results/e2-preparation/2026-11-10T08-28-44-691371Z/report.md`.

- **Native acquisition and source addition:** `results/e2-native/2026-11-10T08-14-00-203308Z/` passes the original 36 signal cases and the additional gateway task for OPC UA, Modbus and S7. A second native fixture supplies another source alias for the same conveyor-speed signal. Before integration the alias is refused; after the generated conversion/context mappings are applied it produces the same public UNS identity and derived warmer decision. The existing alias still works. Each protocol is an independent before/after trial. Original repository mappings and controller code are unchanged. Retained mapping artifacts record changes; human integration effort is explicitly **not measured**.
- **Quality/error policies:** 33 in-process production-code cases cover good, stale, future, malformed/version/type/unit/identity/topic data, unknown aliases, OPC UA bad/uncertain quality and a truncated S7 value. These are policy/decode checks, not live protocol fault injections. Modbus and S7 register mappings do not carry OPC UA source-quality semantics; their timestamps represent adapter acquisition.
- **Paired command and simulated response:** `results/e2-command-response/2026-11-10T08-26-43-346976Z/` passes six selected decisions (22/26 degree C for each protocol), a 99 degree C refusal and restoration. The verified native-derived and source-specific decisions agree. Fresh commands with separate IDs are sent as the authenticated **experiment-driver**, not impersonated as the automatic consumer. The checker reconciles immutable request echoes, completed writer-ledger digests, native OPC UA setpoint readback and simulated temperature response. The temperature must be within 0.75 degree C on three consecutive 0.5-second polls within a 45-second observation window after command completion; these are polls, not three guaranteed independent process updates. This is not an end-to-end latency measurement or a validated thermal-process model.
- The live command path's pod/image identities, ConfigMap hashes and ledger-volume identity are retained before/after. The automatic consumer is paused to avoid competing commands, then its original replica count and original warmer setpoint are restored. No ledger or simulator volume is reset. A first capture exposed a checker assumption about the public result format and remains retained as failed; the fresh run uses the corrected check against the public `approve`/`written_at` response and persisted completed writer record.

The quality evidence and exact source snapshots are retained in the preparation report directory. Native/command captures retain their own source snapshots, records and cleanup evidence. The summary independently re-evaluates the evidence and checks that the paired command case references the same native input. **Whole-study conformance and C04 remain unknown**; selected preparation passing does not replace controlled comparative experiments or establish live automatic decision-to-command lineage.

Reproduce the three stages from the repository root (the command stage temporarily controls the explicitly named research cluster):

```powershell
uv run python scripts/e2_native.py --source-addition
# Substitute the actual output directories printed by the preceding stages.
uv run python scripts/e2_command_response.py --native-run results/e2-native/<native-run> --context kind-eirvah-edge
uv run python scripts/e2_suite.py --native-run results/e2-native/<native-run> --command-run results/e2-command-response/<command-run>
```

Regression validation: **655 passed, 16 skipped**. Ruff, strict mypy for all five E2 modules and `git diff --check` pass. The research controller and all 21 pods were confirmed Ready after the successful run. Current extension changes have not been committed or pushed.
