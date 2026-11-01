"""Prometheus metrics for the S7 simulator."""

from __future__ import annotations

from eirvah_observability.metrics import make_gauge
from prometheus_client.registry import REGISTRY, CollectorRegistry


class SimulatorMetrics:
    def __init__(self, registry: CollectorRegistry = REGISTRY) -> None:
        self._torque = make_gauge(
            "s7_simulator_capper_torque_nm",
            "Capper torque (Nm).",
            labelnames=[],
            registry=registry,
        )
        self._cap_presence = make_gauge(
            "s7_simulator_capper_cap_presence",
            "Capper cap presence: 0=absent 1=present.",
            labelnames=[],
            registry=registry,
        )
        self._rejects = make_gauge(
            "s7_simulator_capper_rejects_per_min",
            "Capper rejects per minute.",
            labelnames=[],
            registry=registry,
        )
        self._layer_count = make_gauge(
            "s7_simulator_palletizer_layer_count",
            "Palletizer current layer count.",
            labelnames=[],
            registry=registry,
        )
        self._pallet_complete = make_gauge(
            "s7_simulator_palletizer_pallet_complete",
            "Palletizer pallet complete flag: 0/1.",
            labelnames=[],
            registry=registry,
        )
        self._cycles_per_hr = make_gauge(
            "s7_simulator_palletizer_cycles_per_hr",
            "Palletizer cycles per hour.",
            labelnames=[],
            registry=registry,
        )

        self._control_motor_running = make_gauge(
            "s7_simulator_motor_running",
            "Capper commanded running: 0/1",
            labelnames=[],
            registry=registry,
        )
        self._control_motor_faulted = make_gauge(
            "s7_simulator_motor_faulted",
            "Capper fault latched: 0/1",
            labelnames=[],
            registry=registry,
        )
        self._control_motor_speed_setpoint_rpm = make_gauge(
            "s7_simulator_motor_speed_setpoint_rpm",
            "Capper speed setpoint (rpm)",
            labelnames=[],
            registry=registry,
        )
        self._control_motor_rpm = make_gauge(
            "s7_simulator_motor_rpm", "Capper actual speed (rpm)", labelnames=[], registry=registry
        )

    def set_control_feedback(self, a: float, b: float, c: float, d: float) -> None:
        self._control_motor_running.set(a)
        self._control_motor_faulted.set(b)
        self._control_motor_speed_setpoint_rpm.set(c)
        self._control_motor_rpm.set(d)

    def set_capper(self, torque: float, cap_presence: int, rejects: int) -> None:
        self._torque.set(torque)
        self._cap_presence.set(cap_presence)
        self._rejects.set(rejects)

    def set_palletizer(self, layer_count: int, pallet_complete: int, cycles_per_hr: int) -> None:
        self._layer_count.set(layer_count)
        self._pallet_complete.set(pallet_complete)
        self._cycles_per_hr.set(cycles_per_hr)
