"""Mean-reverting temperature dynamics (spec §9.2)."""

from __future__ import annotations

from dataclasses import dataclass

from opcua_simulator.rng import SimulatorRNG


@dataclass
class TemperatureDynamics:
    """Per-tick update: T_t = T_{t-1} + alpha*(setpoint - T_{t-1}) + noise + spike."""

    initial: float
    alpha: float
    sigma: float
    rng: SimulatorRNG

    def __post_init__(self) -> None:
        self.value: float = float(self.initial)

    def tick(self, *, setpoint: float, spike_contribution: float) -> float:
        noise = self.rng.gauss(0.0, self.sigma) if self.sigma > 0.0 else 0.0
        self.value = (
            self.value
            + self.alpha * (setpoint - self.value)
            + noise
            + spike_contribution
        )
        return self.value
