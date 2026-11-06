"""Freshness and ordering guard for the selected consumer streams.

Positioned observations use durable producer/epoch/sequence high-water marks.
Timestamp-only legacy mode retains its state within the current process.
"""

from __future__ import annotations

import json
import math
from datetime import datetime
from typing import Any

from eirvah_contracts.signals import SourcePosition
from eirvah_controls.recovery import ConsumerStore


class StateTracker:
    def __init__(
        self,
        max_age_s: float,
        *,
        require_position: bool = False,
        store: ConsumerStore | None = None,
    ) -> None:
        if not math.isfinite(max_age_s) or max_age_s <= 0:
            raise ValueError("positive finite state age required")
        self.max_age_s = max_age_s
        self.require_position = require_position
        self.store = store
        self._positions: dict[str, dict[str, Any]] = {}
        self._latest: dict[str, tuple[datetime, Any]] = {}
        self.last_decision: dict[str, Any] = {"accepted": False, "reason": "not_observed"}

    def update(
        self,
        topic: str,
        value: Any,
        at: datetime,
        now: datetime,
        quality: str,
        position: dict[str, Any] | None = None,
    ) -> bool:
        reason = "accepted"
        if at.tzinfo is None or now.tzinfo is None:
            reason = "invalid_time"
        elif quality != "good":
            reason = "bad_quality"
        elif (now - at).total_seconds() < 0:
            reason = "future"
        elif (now - at).total_seconds() > self.max_age_s:
            reason = "stale"
        elif self.require_position and position is None:
            reason = "missing_position"
        elif position is not None:
            parsed = SourcePosition.model_validate(position)
            body = json.dumps(
                [value, at.isoformat(), quality], sort_keys=True, separators=(",", ":")
            )
            if self.store is None:
                raise RuntimeError("durable position guard required")
            reason = self.store.advance(topic, parsed.model_dump(), body)
            if reason == "accepted":
                self._positions[topic] = parsed.model_dump()
        elif topic in self._latest:
            previous_at, previous_value = self._latest[topic]
            if at < previous_at:
                reason = "out_of_order"
            elif at == previous_at:
                reason = (
                    "duplicate"
                    if type(value) is type(previous_value) and value == previous_value
                    else "timestamp_conflict"
                )
        accepted = reason == "accepted"
        if accepted:
            self._latest[topic] = (at, value)
        self.last_decision = {"accepted": accepted, "reason": reason}
        return accepted

    def snapshot(self, topic: str, now: datetime) -> dict[str, Any]:
        if topic not in self._latest:
            return {"status": "unknown", "value": None, "source_timestamp": None}
        at, value = self._latest[topic]
        age = (now - at).total_seconds() if now.tzinfo is not None else -1
        return {
            "status": "fresh" if 0 <= age <= self.max_age_s else "stale" if age >= 0 else "unknown",
            "value": value,
            "source_timestamp": at.isoformat(),
            "quality": "good",
            **({"position": self._positions[topic]} if topic in self._positions else {}),
        }
