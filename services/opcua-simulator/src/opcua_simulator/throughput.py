"""Throughput dynamics (spec §9.2)."""

from __future__ import annotations

from dataclasses import dataclass

from opcua_simulator.motor import MOTOR_RUNNING, MOTOR_STARTING
from opcua_simulator.rng import SimulatorRNG

_RPM_TO_BPS = 0.0006  # bottles/s per rpm at running


@dataclass
class Throughput:
    rng: SimulatorRNG

    def compute(self, *, motor_state: int, motor_rpm: float) -> float:
        if motor_state == MOTOR_RUNNING:
            return max(0.0, _RPM_TO_BPS * motor_rpm + self.rng.gauss(0.0, 0.05))
        if motor_state == MOTOR_STARTING:
            return max(0.0, _RPM_TO_BPS * motor_rpm)
        return 0.0
