"""Hot-spike trigger: stochastic + OPC UA method (spec §9.2)."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass

from opcua_simulator.rng import SimulatorRNG

SPIKE_AMPLITUDE = 5.0
SPIKE_DECAY = 0.9


@dataclass
class HotSpike:
    rng: SimulatorRNG
    stochastic_probability: float

    def __post_init__(self) -> None:
        self._spike_contribution: float = 0.0
        self._method_triggered: bool = False
        self._triggers: Counter[str] = Counter()

    def trigger_via_method(self) -> None:
        self._method_triggered = True

    def tick(self) -> float:
        self._spike_contribution *= SPIKE_DECAY
        triggered_kind: str | None = None
        if self._method_triggered:
            triggered_kind = "method"
            self._method_triggered = False
        elif self.rng.random() < self.stochastic_probability:
            triggered_kind = "stochastic"
        if triggered_kind is not None:
            self._spike_contribution = SPIKE_AMPLITUDE
            self._triggers[triggered_kind] += 1
        return self._spike_contribution

    def trigger_counts(self) -> dict[str, int]:
        return dict(self._triggers)
