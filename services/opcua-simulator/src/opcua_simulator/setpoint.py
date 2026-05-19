"""Setpoint state + write audit trail (spec §9.2, §9.5)."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class SetpointWrite:
    value: float
    writer_session: str
    at: datetime


@dataclass
class Setpoint:
    initial: float

    def __post_init__(self) -> None:
        self.value: float = float(self.initial)
        self._writes: list[SetpointWrite] = []

    def write(self, *, value: float, writer_session: str, at: datetime) -> None:
        self.value = float(value)
        self._writes.append(
            SetpointWrite(value=float(value), writer_session=writer_session, at=at)
        )

    def write_history(self) -> list[SetpointWrite]:
        return list(self._writes)

    def write_count_by_writer(self) -> dict[str, int]:
        return dict(Counter(w.writer_session for w in self._writes))
