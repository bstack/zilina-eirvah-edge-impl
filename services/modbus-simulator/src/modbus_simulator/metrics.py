"""Prometheus metrics for the Modbus simulator."""
from __future__ import annotations

from eirvah_observability.metrics import make_gauge
from prometheus_client.registry import REGISTRY, CollectorRegistry


class SimulatorMetrics:
    def __init__(self, registry: CollectorRegistry = REGISTRY) -> None:
        self._temperature = make_gauge(
            "modbus_simulator_temperature_celsius",
            "Current temperature from Modbus simulator (°C).",
            labelnames=[],
            registry=registry,
        )
        self._setpoint = make_gauge(
            "modbus_simulator_setpoint_celsius",
            "Current setpoint from Modbus simulator (°C).",
            labelnames=[],
            registry=registry,
        )
        self._motor_state = make_gauge(
            "modbus_simulator_motor_state",
            "Motor state: 0=stopped 1=running.",
            labelnames=[],
            registry=registry,
        )

    def set_temperature(self, value: float) -> None:
        self._temperature.set(value)

    def set_setpoint(self, value: float) -> None:
        self._setpoint.set(value)

    def set_motor_state(self, value: int) -> None:
        self._motor_state.set(value)
