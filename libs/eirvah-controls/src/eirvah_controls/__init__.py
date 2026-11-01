"""Small deterministic simulator controls and device acknowledgement codes.

Status: 1 applied, 2 invalid command/value, 3 state/identity precondition failed,
4 duplicate bottle rejection. These are device outcomes, not cloud approvals.
Operation 4 is simulator-only fault injection, never exposed in normal routes.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field


@dataclass
class MotorControl:
    max_speed: int
    setpoint: int
    running: bool = True
    faulted: bool = False

    @property
    def speed(self) -> int:
        return self.setpoint if self.running and not self.faulted else 0

    def apply(self, operation: int, value: int) -> int:
        if operation == 2:
            if not 0 <= value <= self.max_speed:
                return 2
            self.setpoint = value
        elif operation == 1:
            if value not in (0, 1):
                return 2
            if value and self.faulted:
                return 3
            self.running = bool(value)
        elif operation == 3:
            if value != 1:
                return 2
            if not self.faulted:
                return 3
            self.faulted, self.running = False, False
        elif operation == 4 and value == 1:
            self.faulted, self.running = True, False
        else:
            return 2
        return 1


@dataclass
class ValveControl:
    opened: bool = True

    def apply(self, operation: int, value: int) -> int:
        if operation != 1 or value not in (0, 1):
            return 2
        self.opened = bool(value)
        return 1


@dataclass
class RejectControl:
    window: int = 32
    count: int = 0
    last_bottle: int = 0
    _present: deque[int] = field(default_factory=deque)
    _rejected: set[int] = field(default_factory=set)

    def observe(self, bottle_id: int) -> None:
        if bottle_id not in self._present:
            self._present.append(bottle_id)
        while len(self._present) > self.window:
            self._rejected.discard(self._present.popleft())

    def apply(self, operation: int, value: int) -> int:
        if operation != 1 or not 1 <= value <= 65535:
            return 2
        if value not in self._present:
            return 3
        if value in self._rejected:
            return 4
        self._rejected.add(value)
        self.count = min(65535, self.count + 1)
        self.last_bottle = value
        return 1
