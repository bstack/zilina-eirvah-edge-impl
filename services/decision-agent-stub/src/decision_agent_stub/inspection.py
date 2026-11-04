"""Bounded inspection decisions and evidence-based rejection confirmation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from eirvah_contracts.actuation import ActuationRequest
from eirvah_contracts.inspection import BottleInspection
from eirvah_contracts.ulid import generate_correlation_id
from eirvah_observability.execution import record_command

from decision_agent_stub.config import DecisionAgentSettings

Outcome = tuple[str, str]


class InspectionRule:
    def __init__(self, settings: DecisionAgentSettings, capacity: int = 4096) -> None:
        self.settings = settings
        self.capacity = capacity
        self._seen: dict[str, datetime] = {}
        self._bottles: dict[tuple[str, int], datetime] = {}

    def update(self, event: BottleInspection, now: datetime) -> ActuationRequest | None:
        if not self.settings.inspection_enabled:
            return None
        age = (now - event.inspected_at).total_seconds()
        if not 0 <= age < self.settings.inspection_max_age_s:
            return None
        cutoff = now - timedelta(seconds=self.settings.inspection_max_age_s + 10)
        self._seen = {key: at for key, at in self._seen.items() if at >= cutoff}
        self._bottles = {key: at for key, at in self._bottles.items() if at >= cutoff}
        bottle_key = (event.inspection_id.split(":")[0], event.bottle_id)
        if event.inspection_id in self._seen or bottle_key in self._bottles:
            return None
        if len(self._seen) >= self.capacity:
            return None  # Fail closed; never evict a live deduplication entry to make room.
        self._seen[event.inspection_id] = now
        self._bottles[bottle_key] = now
        if event.passed:
            return None
        return ActuationRequest(
            correlation_id=generate_correlation_id(),
            requester="decision-agent-stub",
            target_uns_topic=self.settings.reject_target_uns_topic,
            requested_value=event.bottle_id,
            value_type="int64",
            reason=f"failed bottle inspection {event.inspection_id}",
            requested_at=now,
            deadline=event.inspected_at + timedelta(seconds=self.settings.inspection_max_age_s),
        )


@dataclass
class PendingRejection:
    request: ActuationRequest
    inspection: BottleInspection
    baseline_count: int
    acknowledged: bool = False


class RejectionTracker:
    def __init__(self, capacity: int = 128) -> None:
        self.capacity = capacity
        self.pending: dict[str, PendingRejection] = {}
        self._feedback: dict[str, tuple[int, datetime]] = {}

    def add(
        self, request: ActuationRequest, inspection: BottleInspection, baseline_count: int
    ) -> None:
        if len(self.pending) >= self.capacity:
            raise ValueError("rejection tracker at capacity")
        self.pending[request.correlation_id] = PendingRejection(request, inspection, baseline_count)

    def expire(self, now: datetime) -> list[Outcome]:
        outcomes = []
        for key, entry in list(self.pending.items()):
            # Allow feedback collection after the command deadline; never retry automatically.
            deadline = entry.request.deadline or entry.request.requested_at
            if now > deadline + timedelta(seconds=10):
                record_command(
                    "decision-agent-stub",
                    "process_response_unresolved",
                    entry.request,
                    inspection_id=entry.inspection.inspection_id,
                    reason="feedback_timeout",
                )
                outcomes.append((entry.inspection.inspection_id, "unknown"))
                del self.pending[key]
        return outcomes

    def result(self, result: dict[str, Any], now: datetime) -> list[Outcome]:
        outcomes = self.expire(now)
        key = result.get("correlation_id")
        if not isinstance(key, str):
            return outcomes
        entry = self.pending.get(key)
        if entry is None:
            return outcomes
        if result.get("decision") == "approve" and result.get("written_at"):
            entry.acknowledged = True
        elif result.get("decision") == "pending":
            return outcomes
        elif result.get("decision") == "unknown":
            record_command(
                "decision-agent-stub",
                "process_response_unresolved",
                entry.request,
                inspection_id=entry.inspection.inspection_id,
                reason="writer_unknown",
            )
            outcomes.append((entry.inspection.inspection_id, "unknown"))
            del self.pending[key]
        else:
            # A definite refusal does not confirm a physical rejection.
            outcomes.append((entry.inspection.inspection_id, "not_confirmed"))
            del self.pending[key]
        return outcomes + self._confirm()

    def feedback(
        self, kind: str, value: int, observed_at: datetime, now: datetime
    ) -> list[Outcome]:
        outcomes = self.expire(now)
        previous = self._feedback.get(kind)
        if previous is None or observed_at >= previous[1]:
            self._feedback[kind] = (value, observed_at)
        return outcomes + self._confirm()

    def _confirm(self) -> list[Outcome]:
        bottle = self._feedback.get("bottle")
        count = self._feedback.get("count")
        if bottle is None or count is None:
            return []
        outcomes = []
        for key, entry in list(self.pending.items()):
            if (
                entry.acknowledged
                and bottle[0] == entry.inspection.bottle_id
                and count[0] > entry.baseline_count
                and bottle[1] >= entry.request.requested_at
                and count[1] >= entry.request.requested_at
            ):
                record_command(
                    "decision-agent-stub",
                    "process_observed",
                    entry.request,
                    inspection_id=entry.inspection.inspection_id,
                    observed_bottle=bottle[0],
                    observed_count=count[0],
                    baseline_count=entry.baseline_count,
                    bottle_observed_at=bottle[1].isoformat(),
                    count_observed_at=count[1].isoformat(),
                    evidence_scope="simulator_feedback",
                )
                outcomes.append((entry.inspection.inspection_id, "confirmed"))
                del self.pending[key]
        return outcomes
