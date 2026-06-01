"""Prometheus metrics for the Modbus multi-station simulator."""
from __future__ import annotations

from eirvah_observability.metrics import make_gauge
from prometheus_client.registry import REGISTRY, CollectorRegistry


class SimulatorMetrics:
    def __init__(self, registry: CollectorRegistry = REGISTRY) -> None:
        self._fill_level = make_gauge(
            "modbus_simulator_fill_level_percent",
            "Filler fill level (%).", labelnames=[], registry=registry,
        )
        self._motor_state = make_gauge(
            "modbus_simulator_motor_state",
            "Filler motor state: 0=stopped 1=running.", labelnames=[], registry=registry,
        )
        self._throughput = make_gauge(
            "modbus_simulator_throughput_bottles_per_second",
            "Filler throughput (bottles/s).", labelnames=[], registry=registry,
        )
        self._belt_speed = make_gauge(
            "modbus_simulator_belt_speed_meters_per_second",
            "Conveyor belt speed (m/s).", labelnames=[], registry=registry,
        )
        self._jam_detected = make_gauge(
            "modbus_simulator_jam_detected",
            "Conveyor jam detected: 0=clear 1=jammed.", labelnames=[], registry=registry,
        )
        self._bottle_count = make_gauge(
            "modbus_simulator_bottle_count_total",
            "Conveyor cumulative bottle count.", labelnames=[], registry=registry,
        )
        self._reject_count = make_gauge(
            "modbus_simulator_reject_count_total",
            "Reject station cumulative reject count.", labelnames=[], registry=registry,
        )
        self._conveyor_active = make_gauge(
            "modbus_simulator_conveyor_active",
            "Reject station conveyor active: 0=stopped 1=running.", labelnames=[], registry=registry,
        )

    def set_fill_level(self, value: float) -> None:
        self._fill_level.set(value)

    def set_motor_state(self, value: int) -> None:
        self._motor_state.set(value)

    def set_throughput(self, value: float) -> None:
        self._throughput.set(value)

    def set_belt_speed(self, value: float) -> None:
        self._belt_speed.set(value)

    def set_jam_detected(self, value: int) -> None:
        self._jam_detected.set(value)

    def set_bottle_count(self, value: int) -> None:
        self._bottle_count.set(value)

    def set_reject_count(self, value: int) -> None:
        self._reject_count.set(value)

    def set_conveyor_active(self, value: int) -> None:
        self._conveyor_active.set(value)
