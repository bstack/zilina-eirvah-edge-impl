"""Deterministic seeded PRNG wrapper (spec §9.4)."""

from __future__ import annotations

import random


class SimulatorRNG:
    def __init__(self, seed: int) -> None:
        self._rng = random.Random(seed)

    def random(self) -> float:
        return self._rng.random()

    def uniform(self, a: float, b: float) -> float:
        return self._rng.uniform(a, b)

    def gauss(self, mu: float, sigma: float) -> float:
        return self._rng.gauss(mu, sigma)

    def randint(self, a: int, b: int) -> int:
        return self._rng.randint(a, b)
