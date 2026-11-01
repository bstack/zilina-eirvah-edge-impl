"""Simplified bottle-conditioning zone with an implicit local regulator."""

from __future__ import annotations

import math
from dataclasses import dataclass

from opcua_simulator.rng import SimulatorRNG


@dataclass
class TemperatureDynamics:
    """First-order setpoint tracking, external cooling and seeded process noise.

    dT/dt = (setpoint - T)/time_constant_s - cooling_rate.
    This models regulated equipment, not heater power or food-processing efficacy.
    """

    initial: float
    time_constant_s: float
    sigma: float
    rng: SimulatorRNG

    def __post_init__(self) -> None:
        if not all(math.isfinite(x) for x in (self.initial, self.time_constant_s, self.sigma)):
            raise ValueError("temperature parameters must be finite")
        if self.time_constant_s <= 0 or self.sigma < 0:
            raise ValueError("time constant must be positive and noise non-negative")
        self.value = float(self.initial)
        self._cooling_rate = 0.0
        self._cooling_remaining_s = 0.0

    def inject_cooling(self, *, rate_celsius_per_s: float, duration_s: float) -> None:
        if not all(math.isfinite(x) and x > 0 for x in (rate_celsius_per_s, duration_s)):
            raise ValueError("cooling rate and duration must be positive and finite")
        # A new injection replaces the previous disturbance; it never changes the setpoint.
        self._cooling_rate = rate_celsius_per_s
        self._cooling_remaining_s = duration_s

    def tick(self, *, setpoint: float, elapsed_s: float, spike_contribution: float = 0.0) -> float:
        if not math.isfinite(elapsed_s) or elapsed_s <= 0:
            raise ValueError("elapsed time must be positive and finite")
        if not math.isfinite(setpoint) or not math.isfinite(spike_contribution):
            raise ValueError("temperature inputs must be finite")
        cooled_s = min(elapsed_s, self._cooling_remaining_s)
        if cooled_s:
            equilibrium = setpoint - self._cooling_rate * self.time_constant_s
            self.value = equilibrium + (self.value - equilibrium) * math.exp(
                -cooled_s / self.time_constant_s
            )
            self._cooling_remaining_s = max(0.0, self._cooling_remaining_s - cooled_s)
        remaining_s = elapsed_s - cooled_s
        self.value = setpoint + (self.value - setpoint) * math.exp(
            -remaining_s / self.time_constant_s
        )
        # Exact variance for a first-order process driven by Gaussian noise.
        noise_scale = self.sigma * math.sqrt(
            self.time_constant_s / 2 * (-math.expm1(-2 * elapsed_s / self.time_constant_s))
        )
        if noise_scale:
            self.value += self.rng.gauss(0.0, noise_scale)
        # Retained only for the explicitly triggered legacy hot-spike demo.
        self.value += spike_contribution
        return self.value
