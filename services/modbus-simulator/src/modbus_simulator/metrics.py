"""Prometheus metrics for the Modbus filler simulator."""
from __future__ import annotations

from eirvah_observability.metrics import make_gauge
from prometheus_client.registry import REGISTRY, CollectorRegistry


class SimulatorMetrics:
    def __init__(self, registry: CollectorRegistry = REGISTRY) -> None:
        self._fill_level = make_gauge(
            "modbus_simulator_fill_level_percent",
            "Current fill level from Modbus filler simulator (%).",
            labelnames=[],
            registry=registry,
        )
        self._motor_state = make_gauge(
            "modbus_simulator_motor_state",
            "Motor state: 0=stopped 1=running.",
            labelnames=[],
            registry=registry,
        )
        self._throughput = make_gauge(
            "modbus_simulator_throughput_bottles_per_second",
            "Current throughput from Modbus filler simulator (bottles/s).",
            labelnames=[],
            registry=registry,
        )

    def set_fill_level(self, value: float) -> None:
        self._fill_level.set(value)

    def set_motor_state(self, value: int) -> None:
        self._motor_state.set(value)

    def set_throughput(self, value: float) -> None:
        self._throughput.set(value)
