# Five independent simulated control targets

Stage 1 adds native-protocol controls to the existing temperature demonstration. It does not implement the later cloud-approval contract, durable deduplication, a command load generator or the paper's full conformance suite.

| Target | Protocol | Commands and simulated limits | Observable response |
|---|---|---|---|
| Temperature unit | OPC UA | Existing 20–30 °C setpoint policy | Existing setpoint and temperature dynamics |
| Conveyor motor | Modbus TCP | Start/stop; speed 0–0.80 m/s in 0.01 m/s steps; reset fault | Belt speed, running state, latched jam/fault, speed setpoint and bottle count |
| Filler valve | Modbus TCP | Open/close | Valve feedback; existing simulated fill-level evolution pauses when closed and filler throughput becomes zero |
| Capper motor | S7 | Start/stop; integer speed 0–1800 rpm; reset fault | Running/fault state, setpoint and actual speed; simulated torque becomes zero when stopped |
| Reject mechanism | Modbus TCP | Reject one recently observed bottle ID, 1–65535 | Last rejected ID and cumulative rejection count |

These are configurable demonstration choices, not equipment specifications, acceptance budgets or a physical plant model. Initial conveyor/capper state is running, with 0.50 m/s and 1500 rpm respectively; the filler valve starts open. The existing bottler motor remains its original autonomous demonstration and is distinct from these two controlled motors.

## State and target isolation

Every controlled target owns separate state. Motor start is rejected while faulted. Changing speed while stopped stores the setpoint without starting the motor. Reset is permitted only while faulted and leaves the motor stopped; a separate start is required. Invalid commands do not change state. Simulator mailbox operation 4 with value 1 provides deterministic fault injection for fixtures; it has no normal UNS command route.

The conveyor emits synthetic bottle IDs while moving. The reject mechanism accepts only IDs among the last 32 observed bottles, and suppresses a second rejection of that bottle in this window. This state is in memory, clears on restart and does not establish durable command deduplication or exactly-once physical effects. Bottle IDs wrap through 1–65535; they are local simulator identities, not globally unique business identities. Rejection counts saturate at 65535. Declare/reset the simulation window for experiments.

## Commands through the existing application path

After building and deploying the changed services/configuration, open a separate terminal:

```powershell
kubectl --context kind-eirvah-edge -n eirvah-edge port-forward svc/rabbitmq 5672:5672
```

From the repository root:

```powershell
uv run python scripts/control.py --target temperature --action set --value 25
uv run python scripts/control.py --target conveyor --action stop
uv run python scripts/control.py --target conveyor --action speed --value 0.6
uv run python scripts/control.py --target conveyor --action start
uv run python scripts/control.py --target filler --action close
uv run python scripts/control.py --target filler --action open
uv run python scripts/control.py --target capper --action speed --value 1200
uv run python scripts/control.py --target capper --action stop
uv run python scripts/control.py --target capper --action start
```

Reset a faulted conveyor or capper using `--action reset`. Resetting a healthy motor should be rejected. For the reject target, supply a freshly observed Conveyor BottleCount value:

```powershell
uv run python scripts/control.py --target reject --action reject --value 42
```

The example ID is not guaranteed to be present. Do not retry an uncertain rejection automatically. The CLI prints the command ID and waits for the correlated application result. This is a single-command functional tool, not the planned controlled-workload generator. The `experiment-driver` requester is allowed only by the explicit local policies; the existing decision stub remains a temperature controller.

## Native command and feedback mapping

`config/native-actuation-targets.yaml` and its deployed copy bind each command topic to a trusted endpoint, operation, type, range and encoding. Clients do not supply arbitrary protocol addresses. The validator enforces requester, JSON value type and range. The writer repeats encoding/range checks and requires a matching device acknowledgement, rather than treating a transport write alone as execution success.

For Modbus, each mailbox is three holding registers: sequence, operation, value. The writer sends operation/value first, then commits the sequence. The simulator applies a newly committed sequence once and stores sequence/status in the acknowledgement registers.

| Target | Command base | Acknowledgement base |
|---|---|---|
| Conveyor | 20 | 50 |
| Filler valve | 24 | 52 |
| Reject mechanism | 28 | 54 |

The S7 capper mailbox uses DB3 bytes 0–5; acknowledgement is at bytes 6–9. Payload is written before the sequence. DB3 feedback at bytes 10/12/14/16 contains running, faulted, speed setpoint and actual speed. Existing DB1/DB2 measurement layouts are preserved. Status codes: 1 applied, 2 invalid, 3 state/identity precondition failed, 4 duplicate bottle rejection.

The writer currently serializes commands and has one replica. Do not independently scale physical-write ownership without the later coordination policy. Mailbox sequence numbers are short-lived transport correlation, not durable application command identities. Device acknowledgement timeout is uncertain; the existing orchestrator's complete unknown-outcome and retry semantics remain work for the command-contract stage.

Modbus feedback addresses 8–11 and the S7 DB3 feedback are polled by the existing subscribers. Added conversion rules and SOSA ontology entries route them through the normal UNS publication path. New Prometheus control gauges and the Controlled actuators dashboard row expose the corresponding state. Temperature remains the existing closed-loop baseline.

## Validation and deployment boundary

Tests cover separate motor/valve state, range/type rejection, start/reset interlocks, bottle identity/window handling, configuration bindings, feedback contextualization and real loopback Modbus/S7 writes. Fault injection is deterministic in fixtures. Kubernetes deployment and cross-service AMQP behaviour still require the local functional rehearsal after rebuilding the relevant images.

Rebuild `modbus-simulator`, `s7-simulator`, `actuation-signal-publisher` and `actuation-event-validator`; load those images into kind. Apply the local Kustomize overlay to update policies, native routes, subscriber maps, ontology, conversion rules and dashboards. Restart any changed image using the same `:local` tag. Do not delete the cluster or rebuild unrelated images solely for this stage. Preserve results and use the explicit `kind-eirvah-edge` context.
