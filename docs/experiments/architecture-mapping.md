# Paper 1 architecture implementation mapping

This mapping connects the current research implementation to Definitions 1–8 in study plan v2.1 and checks C01–C13 in the conformance matrix. The machine-readable companion is [local-control.json](../../config/architecture/local-control.json).

It covers the five controls, the warmer production-mode rule, automatic inspection and rejection, and observation responsibilities. It is an initial source mapping of the local baseline, not the whole proposal architecture or a claim of formal conformance. No service behaviour or deployment configuration changes are required to use it.

## Read the two views together

- **Components** identify deployed services and external experiment drivers, with source references.
- **Vertices and edges** describe logical responsibilities and permitted information flows. A logical vertex can map to several services, and a service can participate in more than one responsibility.
- **Channels** describe actual MQTT, AMQP, NATS and native protocol connections. Each logical edge names the channels that realize it; an internal request/reply exchange is not automatically a new business interaction.
- **Contracts** identify the information definition and flow kind. A null wire version means no version is established in that wire format; it does not mean version 1.0. Bindings are marked partial because semantic, compatibility and delivery policies remain incomplete.
- **Required routes** describe selected paths that must exist and the roles they must include. They describe the declared graph, not an access-control boundary.
- **Placement** records the documented single-cluster baseline independently of the logical graph. It has not been verified against a running cluster by these checks.
- **Gaps** retain the unfinished implementation work rather than silently treating the proposed design as implemented.

The file's `format_version` versions the manifest structure. It does not change all message contract versions together. The recorded source baseline is commit `556f512ff0e479acd3308ccde53fc4733009721d`. The topology mapping was committed afterward; authenticated command handoffs are the subsequent implementation described in [command approval](command-approval.md).

## Map the six functional groupings

| Grouping | Current realization | Formal roles and interpretation |
| --- | --- | --- |
| v1 Physical system | OPC UA, Modbus and S7 simulators | `physical`; simulated equipment rather than validated plant physics |
| v2 Integration and contextualization | Three protocol subscribers, conversion, auto contextualization and its orchestrator | `adapter` and `contextualizer`; workers and orchestration jointly realize the integration vertex |
| v3 UNS information space | MQTT publisher and Mosquitto; RabbitMQ application request/outcome paths and AMQP bridge; topic/ontology bindings | `namespace`; the logical UNS spans these responsibilities and bindings, not just one broker |
| v4 Decision and supervision | Fixed rules in `decision-agent-stub`, manual control script and experiment driver | `decision` and `consumer` where applicable; the disturbance driver is a test stimulus source |
| v5 Command validation and execution | Actuation orchestrator, validator and signal publisher/native writers | `executor`; admission and writing are distinct vertices, both currently local |
| v6 Observability | Instrumentation, Prometheus, Grafana and kube-state-metrics | `observer`; emitting components retain their own roles |

NATS supports internal communication across groupings. It is recorded as infrastructure rather than assigned a new formal role. An observation edge expresses an observation responsibility; it does not assert that every component has every metric, centralized logs or a complete trace.

The `profile` section records the selected roles, role-to-layer mapping, four flow kinds and permitted role/flow/role triples from the study profile. It does not introduce an additional orchestrator or broker role into the mathematical definitions.

## Current paths

### Industrial state and inspection

```text
OPC UA / Modbus / S7 simulator
  -> matching protocol subscriber
  -> NATS uns.ingress.raw
  -> contextualization orchestrator
       -> uns.work.convert       -> data-converter
       -> uns.work.contextualize -> uns-auto-contextualizer
       -> uns.work.publish       -> mqtt-uns-publisher
  -> MQTT seven-level UNS topic
  -> subscribed consumer
```

The integration vertex groups the orchestrator and its conversion/contextualization workers. The state namespace vertex groups publication and broker responsibilities. Return messages from workers refine this logical flow; they are not additional industrial events by default.

Industrial state and inspection events share parts of the implementation pipeline but have different logical flow kinds. Inspection starts as an atomic Modbus register snapshot, is decoded into a versioned `BottleInspection` JSON string inside a raw signal envelope, then travels as a value inside a SOSA observation. Native snapshot, raw envelope and canonical inspection bindings are recorded separately. Full end-to-end version and event-policy enforcement remains incomplete.

The decision agent subscribes to four selected topics:

- Conveyor speed setpoint.
- Bottle inspection event.
- Last rejected bottle identity.
- Rejection count.

It does not subscribe to every OPC UA or S7 measurement. State publication routes for those protocols therefore end at the state namespace in the required-route list. Warmer state remains observable through publication and simulator metrics, without being the input to the current warmer decision rule.

### Commands and outcomes

```text
Decision agent or manual control
  -> AMQP eirvah.actuation.commands exchange / eirvah.actuation.requests queue
  -> amqp-actuation-event-subscriber
  -> NATS act.ingress.requested
  -> actuation-control-orchestrator
       -> act.work.validate     -> actuation-event-validator
       -> act.work.write_signal -> actuation-signal-publisher
  -> OPC UA / Modbus / S7 write to simulator

Actuation orchestrator
  -> eirvah.actuation.results exchange / actuation.result routing key
  -> decision agent or manual caller
```

RabbitMQ validates the publisher user ID. The bridge derives requester identity from that property and signs the ingress command. The validator verifies ingress evidence before policy checks, then signs approval bound to the complete command and intended writer. The writer verifies approval on receipt and immediately before native writes, including after connection setup. The post-write `approve` result remains a separate outcome. See [command approval](command-approval.md) for trust assumptions and deployment.

`written_at` now records the writer’s acknowledged completion and is preserved when replaying a retained result. It is not a native write initiation timestamp or proof of physical process success. Writer reply loss is reported as unknown, not a known rejection. See [command recovery](command-recovery.md).

## Selected scenarios and controls

| Scenario or control | Decision or command source | Native target | Current feedback |
| --- | --- | --- | --- |
| Warmer production mode | Conveyor speed selects a normal/high recipe in the fixed rule | OPC UA temperature setpoint | Published warmer state and simulator metrics; not temperature-based supervisory feedback |
| Conveyor | Manual start, stop, speed and fault reset | Modbus belt controls | AMQP outcome; separately published state |
| Filler | Manual open and close | Modbus valve | AMQP outcome; separately published state |
| Capper | Manual start, stop, speed and fault reset | S7 motor controls | AMQP outcome; separately published state |
| Automatic rejection | Fresh failed inspection triggers the fixed rule | Modbus bottle-specific reject operation | AMQP result plus matching last rejected bottle and counter |

The manifest also includes manual commands for temperature and rejection. A route from the decision vertex to each protocol represents available command connectivity, not an implemented automatic decision rule for every control. The scenario list states which rules are actually automatic.

The manual control script waits for an application outcome; it does not independently verify each final process state. Such verification belongs to the repeatable scenario runner.

## Contracts and namespace bindings

The catalogue points to the existing models and configuration:

- `RawSignalEnvelope` and normalized signal definitions for acquisition and integration.
- `SOSAObservation` for public industrial state; no explicit schema version is emitted in that format today.
- `BottleInspection` version `1.0` for the decoded inspection occurrence.
- `ActuationRequest` version `1.0` for commands.
- `ActuationApproveResult` and `ActuationRejectResult`, inheriting the request version, for application outcomes.
- Native command mappings and operational observation formats with their current limitations.

A valid seven-level topic is not proof of registered industrial identity or correct units and meaning. The namespace bindings section references the ontology, native write maps and deployed policy. Complete identity/description checks and receiver compatibility remain to be added. The manifest records their locations; it does not claim that the full shared information space has been validated.

Native acquisition, final device writes and operational observations are declared boundaries. The intended cloud admission to edge writer connection is an internal v5 handoff. The complete decision-to-execution route must still use the logical UNS.

## Open gaps and next work

| Gap | Relevant checks | What remains |
| --- | --- | --- |
| Bound admission and authority | C07 / CR5 | Implemented with validated broker identity and HMAC handoffs; verify deployed behaviour and retain the trusted-service assumptions |
| Deadline at execution | C08 / CR5 | Implemented at the writer and native write boundaries; distributed clock policy and E0 deployment evidence remain |
| Immutable IDs and recovery | C09 / CR6 | Implemented with a single-writer SQLite ledger and PVC; verify deployment/restart behaviour and retain the distinction from bottle-effect guards |
| Uncertain outcomes | C10 / CR6 | Typed pending/unknown outcomes and durable initiation state are implemented; full execution traces and physical reconciliation remain |
| Event binding and coverage | C03, C06 / CR4 | Enforce declared event contract/version semantics and expose missed inspection coverage |
| Execution evidence | C12 / CR8 | Complete attributable action records and timing uncertainty rules |
| Distributed placement | C13 | Separate edge/cloud selections and connections; the current mapping remains single-cluster |
| Runtime boundary enforcement | C01–C02 | Detect or prevent undeclared command bypasses; graph traversal alone is insufficient |

Cooling and fault injection are experiment-only access paths, not admitted production command routes. Administrative access and direct simulator/broker access are outside the permitted business graph. They are listed as exclusions so the graph is not mistaken for proof that such connections are impossible.

Command approval and writer verification are implemented in code. Deployment now requires the generated security Secret and updated command services; a cluster rebuild is unnecessary. [Duplicate/recovery policy](command-recovery.md) is now implemented with persistent writer storage. Full execution traces and runtime E0 checks remain next steps.

## Check the mapping

From the research repository root:

```powershell
uv run python -m pytest tests/scripts/test_architecture_manifest.py -q
```

The six checks cover:

1. Unique IDs, valid source references, typed logical edges, role compatibility and contract flow kinds.
2. Nonempty required routes and absence of namespace/executor bypasses in the declared graph.
3. Five control targets matching deployed admission policies and native mappings.
4. Pipeline stage subjects and dead-letter subjects matching deployed pipeline configuration.
5. Complete base deployment inventory and explicit gaps/placement status.
6. Selected controller topic defaults, scenario routes and existing contract versions matching code models.

These are offline mapping consistency checks. They do not contact a cluster, enumerate all runtime connections, authenticate callers, check every deployment override, execute the conformance cases, or prove Definition 7's universal condition. Keep `conformance_verdict` as `not_assessed` until scoped runtime checks supply evidence.

When services, mappings or contracts change, update this manifest and its source references. When placement changes, update the placement record without silently changing logical responsibilities. The E0 checker and scenario runner can build on this representation in later steps.

## Scoped E0 evaluator

The [E0 checker](conformance.md) now evaluates the declared graph and retained command evidence against a separate pinned reference profile. It reports pass, fail or unknown per subcheck and C01–C13 rule family, with not-run experiments and evidence gaps explicit. It does not change this manifest into a claim of verified deployment or universal conformance.
