# Bottle-conditioning warmer: simplified experimental control

## Purpose and evidence boundary

Temperature is the fifth independent control function alongside conveyor, filler, capper and rejection. It represents a conditioning zone used before labelling cold-filled bottles. Commercial container warmers address condensation and packaging quality; see [Krones LinaTherm](https://www.krones.com/en/products/machines/warmer.php). That source supports the use case, not our numerical parameters or a validated commercial process model.

The existing temperature and setpoint OPC UA nodes, UNS paths and Prometheus names are retained. Grafana labels identify the warmer. Temperature means zone temperature, not the temperature of every bottle. There is no model of pasteurization, sterilization, food safety, humidity/dew point or individual bottle heat transfer.

## Equipment model

The simulated equipment includes an implicit local temperature regulator:

`dT/dt = (S - T) / tau - C`

Here T is measured zone temperature, S is the commanded recipe setpoint, tau is the response time constant in seconds and C is an external cooling rate in degrees Celsius per second. The update integrates the first-order equation over elapsed monotonic time, splitting an interval when the cooling load ends. Seeded Gaussian process noise has elapsed-time-dependent variance. Exact tick-size equivalence is tested with noise disabled; different noisy sampling schedules do not produce identical trajectories.

Defaults are **experimental assumptions**, not industrial specifications:

| Parameter | Default | Configuration |
|---|---|---|
| Initial zone temperature | 22 degrees Celsius | Address-space initial value |
| Time constant | 10 seconds | `OPCUA_SIMULATOR_TEMPERATURE_TIME_CONSTANT_S` |
| Noise intensity | 0.05 degrees Celsius / square-root second | `OPCUA_SIMULATOR_TEMPERATURE_NOISE` |
| Injected cooling rate | 0.5 degrees Celsius / second | `OPCUA_SIMULATOR_COOLING_RATE_CELSIUS_PER_S` |
| Cooling duration | 10 seconds | `OPCUA_SIMULATOR_COOLING_DURATION_S` |

Calling `bottler/TriggerCoolingLoad` starts a repeatable cooling interval representing an incoming cold-bottle load. Another call replaces the pending load rather than accumulating it. This is simulator-only disturbance injection, not an application actuation route. It never writes the temperature setpoint. Cooling is currently injected by the scenario, not calculated from conveyor speed, bottle mass or measured throughput.

The earlier `TriggerHotSpike` method remains for historical fault demonstrations; stochastic hot spikes are disabled in the base configuration. Its per-tick behaviour is not part of the new time-based warmer workload.

## Supervisory recipe selection

The decision service reads the existing UNS **conveyor speed-setpoint** observation, not a measured production throughput:

| Observed speed setpoint | Recipe temperature |
|---|---|
| At or below 0.5 m/s | Normal: 22 degrees Celsius |
| At or above 0.6 m/s | High: 26 degrees Celsius |
| Between the bands | Keep the previous mode; emit no command |

These speed bands and temperatures are configurable study assumptions. A higher recipe at higher speed represents a proposed compensation policy, not a demonstrated optimum. The measured throughput and thermal adequacy of the plant are not inferred from this table.

Each mode transition (including initial selection) emits one fresh command ID through AMQP, the existing validator and OPC UA writer. Repeated observations within the same mode do not keep overwriting manual settings. Bad/uncertain quality, wrong unit, invalid speed, missing or stale timestamps, future timestamps and older observations are ignored. The default observation age budget is 5 seconds and assumes synchronized clocks. The rule's freshness budget needs reassessment for distributed environments.

The local thermal feedback is implicit in the equipment model. The external rule is **supervisory recipe selection from production conditions**, not a new temperature-feedback controller. It no longer uses the old 26-degree/30-second threshold trigger. A cooling disturbance alone should recover through the local regulator without generating a new supervisory command.

The decision service does not yet reconcile command outcomes or implement durable retries. It emits once per mode transition; rejected, failed or uncertain commands require inspection. Mode state is in memory and resets when the service restarts, so restart can emit the selected recipe again. A published command does not by itself establish a device write or process response. Inspect the correlated application result and measured setpoint/temperature separately.

Disable supervisory recipe changes for manual temperature checks or a fixed-setpoint comparison using `DECISION_AGENT_STUB_ENABLED=false`. Do not compare a naturally recovering disturbance with an unchanged 22-degree command and claim that the command caused recovery.

## Repeatable checks

Tests cover:

- deterministic setpoint tracking with identical elapsed time at different tick sizes;
- a cooling interval, including a fraction of a tick, that preserves the setpoint;
- recovery without any new command versus recovery to a newly commanded recipe;
- seeded noise replay under the same sampling schedule;
- mode transitions, repeated observations, hysteresis, disabled supervision and invalid/stale observations;
- a real loopback OPC UA server: setpoint write, measured response, cooling method and subsequent recovery;
- the demo sends conveyor commands through AMQP and does not inject cooling after an unconfirmed command.

These are local engineering checks, not measured Kubernetes scalability results or validated industrial physics.

## Deployment and local rehearsal

Rebuild and load only `opcua-simulator:local` and `decision-agent-stub:local`, apply the local Kustomize overlay, then restart both deployments and wait for rollout. No cluster recreation is needed. The overlay also updates warmer labels in Grafana. Existing brokers, native writers and protocol adapters are reused. Use the explicit `kind-eirvah-edge` context for deployment.

For a manual rehearsal, open separate port-forwards:

```powershell
kubectl --context kind-eirvah-edge -n eirvah-edge port-forward svc/opcua-simulator 4840:4840
kubectl --context kind-eirvah-edge -n eirvah-edge port-forward svc/rabbitmq 5672:5672
```

Then run:

```powershell
uv run python scripts/disturbance.py --interval 120
```

This alternates conveyor speed-setpoint commands between 0.7 and 0.4 m/s. Each confirmed conveyor write is followed by a cooling injection. Watch the separate warmer command and the new zone setpoint (26 then 22 degrees), the transient cooling and gradual response in Grafana. The tool does not start or reset a stopped/faulted conveyor and does not prove that physical throughput changed. Cooling injection is scenario-driven. Stop with Ctrl+C; the final mode remains selected. No automatic restoration or retry occurs after an unknown outcome.

`harness.py run --experiment a --duration 240` uses this same scenario with metric collection and port-forwards (close conflicting manual forwards first). Its `ok` status still describes action/collection completion, not an automatic thermal-conformance verdict. Its historical results used the earlier threshold controller and are not comparable without qualification. Retain command results and inspect setpoint and temperature separately.

Automatic sampled bottle inspection and rejection are described separately in [automatic inspection](automatic-inspection.md); the warmer scenario remains independent.
