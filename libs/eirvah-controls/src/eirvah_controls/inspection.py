"""Deterministic sampled inspection of bottles actually observed by the simulator."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class InspectionStream:
    session: int
    every_n_bottles: int = 4
    fail_every_n_inspections: int = 5
    inspected: int = field(default=0, init=False)
    failed: int = field(default=0, init=False)
    _bottles: int = field(default=0, init=False)
    _sequence: int = field(default=0, init=False)
    _snapshot: list[int] = field(default_factory=lambda: [0] * 10, init=False)

    def __post_init__(self) -> None:
        if not 0 < self.session < 2**64:
            raise ValueError("inspection session must be a nonzero unsigned 64-bit identity")
        if self.every_n_bottles < 1 or self.fail_every_n_inspections < 1:
            raise ValueError("inspection intervals must be positive")

    def observe(self, bottle_id: int, now: datetime) -> None:
        if not 1 <= bottle_id <= 65535 or now.tzinfo is None:
            raise ValueError("inspection requires a valid bottle ID and aware timestamp")
        self._bottles += 1
        if self._bottles % self.every_n_bottles:
            return
        self.inspected += 1
        self._sequence += 1
        if self._sequence > 65535:
            self._sequence = 1
            self.session = self.session % (2**64 - 1) + 1
        failed = self.inspected % self.fail_every_n_inspections == 0
        self.failed += int(failed)
        timestamp_ms = int(now.timestamp() * 1000)
        if not 0 <= timestamp_ms < 2**48:
            raise ValueError("inspection timestamp does not fit the wire representation")
        self._snapshot = [
            *[(self.session >> shift) & 65535 for shift in (48, 32, 16, 0)],
            self._sequence,
            bottle_id,
            2 if failed else 1,
            *[(timestamp_ms >> shift) & 65535 for shift in (32, 16, 0)],
        ]

    def registers(self) -> list[int]:
        return list(self._snapshot)
