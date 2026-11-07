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

1. Exercise equivalent inputs through actual OPC UA, Modbus and S7 acquisition paths in an isolated test selection, without competing with the automatic plant controller.
2. Reconcile emitted observations across the intended NATS and public MQTT boundaries, then check command outcomes and independently observed device responses where selected.
3. Add the defined source-addition task and record actual mapping/configuration/code changes, effort and errors. The current baseline is preparation, not a measured claim of less integration effort.
4. Extend selected quality, timestamp, malformed input, counter-range and event-contract scenarios. Register polling cannot be assumed to supply OPC UA quality or source timestamp semantics.
5. Run equivalent repetitions on controlled infrastructure and analyse the results alongside E0 checks. This runner is neither an E1 throughput benchmark nor an E3 deployment-flexibility result.

These fixtures are synthetic and tied to the selected simulated line. They are not presented as a published industrial dataset or a validated physical process model.


## Local verification - 7 November 2026

`results/interoperability/2026-11-07T05-56-29-062718Z/` retained a passing 102-case matrix (including six expected range refusals), two attribution/completeness checks and six passing paired upper-bound checks. The unrounded S7 value was 0.800000011920929; both normalized variants produced 0.8 and the expected warmer request. C04 remains unknown.

Final regression validation: **601 passed, 16 skipped**. Ruff, strict mypy for the three changed/new runtime modules and `git diff --check` passed. The existing feedback fixture was adjusted from an out-of-range 1 m/s to a valid 0.4 m/s; dedicated tests retain over-range rejection coverage. These checks used local production code in-process. The cluster was not redeployed and the changes have not been committed or pushed.
