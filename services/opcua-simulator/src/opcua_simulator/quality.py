"""Configurable per-node quality emission (spec §9.3)."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Literal

from opcua_simulator.rng import SimulatorRNG

Quality = Literal["good", "uncertain", "bad"]


@dataclass
class QualityEmitter:
    rng: SimulatorRNG
    bad_quality_pct: float
    uncertain_quality_pct: float

    def __post_init__(self) -> None:
        self._counts: Counter[str] = Counter()

    def next(self) -> Quality:
        r = self.rng.random()
        if r < self.bad_quality_pct:
            self._counts["bad"] += 1
            return "bad"
        if r < self.bad_quality_pct + self.uncertain_quality_pct:
            self._counts["uncertain"] += 1
            return "uncertain"
        self._counts["good"] += 1
        return "good"

    def emission_counts(self) -> dict[str, int]:
        return dict(self._counts)
