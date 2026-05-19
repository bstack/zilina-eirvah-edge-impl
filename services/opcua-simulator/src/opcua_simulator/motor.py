"""Motor state machine + RPM dynamics (spec §9.2)."""

from __future__ import annotations

from dataclasses import dataclass

from opcua_simulator.rng import SimulatorRNG

MOTOR_STOPPED = 0
MOTOR_STARTING = 1
MOTOR_RUNNING = 2
MOTOR_FAULT = 3

_STOPPED_TO_STARTING_MS = 5_000
_STARTING_TO_RUNNING_MS = 3_000
_FAULT_TO_STOPPED_MS = 10_000

_TARGET_RPM = 1500.0
_RPM_NOISE = 50.0


@dataclass
class Motor:
    rng: SimulatorRNG
    tick_ms: int
    fault_probability: float

    def __post_init__(self) -> None:
        self.state: int = MOTOR_STOPPED
        self.rpm: float = 0.0
        self._time_in_state_ms: int = 0

    def tick(self) -> None:
        self._time_in_state_ms += self.tick_ms
        if self.state == MOTOR_STOPPED:
            if self._time_in_state_ms >= _STOPPED_TO_STARTING_MS:
                self._enter(MOTOR_STARTING)
        elif self.state == MOTOR_STARTING:
            ramp = min(1.0, self._time_in_state_ms / _STARTING_TO_RUNNING_MS)
            self.rpm = ramp * _TARGET_RPM
            if self._time_in_state_ms >= _STARTING_TO_RUNNING_MS:
                self._enter(MOTOR_RUNNING)
        elif self.state == MOTOR_RUNNING:
            self.rpm = _TARGET_RPM + self.rng.gauss(0.0, _RPM_NOISE)
            if self.rng.random() < self.fault_probability:
                self._enter(MOTOR_FAULT)
        elif self.state == MOTOR_FAULT and self._time_in_state_ms >= _FAULT_TO_STOPPED_MS:
            self._enter(MOTOR_STOPPED)

    def _enter(self, state: int) -> None:
        self.state = state
        self._time_in_state_ms = 0
        if state in (MOTOR_STOPPED, MOTOR_FAULT) or state == MOTOR_STARTING:
            self.rpm = 0.0
