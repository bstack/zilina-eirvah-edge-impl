"""Prometheus surface for the simulator (spec §9.5)."""

from __future__ import annotations

from eirvah_observability.metrics import make_counter, make_gauge
from prometheus_client.registry import REGISTRY, CollectorRegistry

_ISA95_LABELS = ("enterprise", "site", "area", "line", "cell", "equipment")


class SimulatorMetrics:
    """Facade for every Prometheus metric the simulator emits."""

    def __init__(self, registry: CollectorRegistry = REGISTRY) -> None:
        self._temperature = make_gauge(
            "simulator_temperature_celsius",
            "Current temperature reading from the simulator (°C).",
            labelnames=_ISA95_LABELS,
            registry=registry,
        )
        self._setpoint = make_gauge(
            "simulator_setpoint_celsius",
            "Current setpoint value (°C).",
            labelnames=_ISA95_LABELS,
            registry=registry,
        )
        self._throughput = make_gauge(
            "simulator_throughput_bottles_per_second",
            "Current throughput (bottles/s).",
            labelnames=_ISA95_LABELS,
            registry=registry,
        )
        self._motor_state = make_gauge(
            "simulator_motor_state",
            "Motor state: 0=stopped 1=starting 2=running 3=fault.",
            labelnames=_ISA95_LABELS,
            registry=registry,
        )
        self._motor_rpm = make_gauge(
            "simulator_motor_rpm",
            "Motor RPM.",
            labelnames=_ISA95_LABELS,
            registry=registry,
        )
        self._quality_count = make_counter(
            "simulator_quality_count",
            "Samples emitted per quality bucket.",
            labelnames=(*_ISA95_LABELS, "quality"),
            registry=registry,
        )
        self._setpoint_writes = make_counter(
            "simulator_setpoint_writes",
            "Setpoint writes received from the EirVah pipeline.",
            labelnames=("writer",),
            registry=registry,
        )
        self._hot_spikes = make_counter(
            "simulator_hot_spikes",
            "Hot-spike triggers fired, by source.",
            labelnames=("trigger",),
            registry=registry,
        )

    def set_temperature(self, labels: dict[str, str], value: float) -> None:
        self._temperature.labels(**labels).set(value)

    def set_setpoint(self, labels: dict[str, str], value: float) -> None:
        self._setpoint.labels(**labels).set(value)

    def set_throughput(self, labels: dict[str, str], value: float) -> None:
        self._throughput.labels(**labels).set(value)

    def set_motor_state(self, labels: dict[str, str], state: int) -> None:
        self._motor_state.labels(**labels).set(state)

    def set_motor_rpm(self, labels: dict[str, str], rpm: float) -> None:
        self._motor_rpm.labels(**labels).set(rpm)

    def inc_quality(self, *, labels: dict[str, str], quality: str) -> None:
        self._quality_count.labels(**labels, quality=quality).inc()

    def inc_setpoint_write(self, *, writer: str) -> None:
        self._setpoint_writes.labels(writer=writer).inc()

    def inc_hot_spike(self, *, trigger: str) -> None:
        self._hot_spikes.labels(trigger=trigger).inc()
