"""Mean-reverting quality rate dynamics (e.g. inspection pass rate, label alignment)."""
from __future__ import annotations

from dataclasses import dataclass, field

from opcua_simulator.rng import SimulatorRNG


@dataclass
class QualityRateDynamics:
    """Simulates a quality percentage that wanders near a target value.

    Uses the same mean-reversion formula as TemperatureDynamics but
    with no external setpoint — the target is fixed at construction.
    """

    target: float       # e.g. 98.0 for 98%
    alpha: float = 0.1  # convergence speed
    sigma: float = 0.3  # noise magnitude
    rng: SimulatorRNG = field(default_factory=lambda: SimulatorRNG(seed=0))

    def __post_init__(self) -> None:
        self.value: float = self.target

    def tick(self) -> float:
        noise = self.rng.gauss(0.0, self.sigma) if self.sigma > 0.0 else 0.0
        self.value = self.value + self.alpha * (self.target - self.value) + noise
        self.value = max(85.0, min(100.0, self.value))
        return self.value
