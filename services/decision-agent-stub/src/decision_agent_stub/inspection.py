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
        if capacity < 1:
            raise ValueError("positive inspection capacity required")
        self._seen: dict[str, tuple[datetime, BottleInspection]] = {}
        self.last_decision: dict[str, Any] = {
            "accepted": False,
            "reason": "not_observed",
            "missing_sequences": 0,
            "late_sequence": False,
        }
        self._bottles: dict[tuple[str, int], datetime] = {}
        self._heads: dict[str, tuple[int, datetime, datetime]] = {}

    def update(self, event: BottleInspection, now: datetime) -> ActuationRequest | None:
        self.last_decision = {
            "accepted": False,
            "reason": "disabled",
            "missing_sequences": 0,
            "late_sequence": False,
        }
        if not self.settings.inspection_enabled:
            return None
        age = (now - event.inspected_at).total_seconds()
        if not 0 <= age < self.settings.inspection_max_age_s:
            self.last_decision["reason"] = "future" if age < 0 else "stale"
            return None
        cutoff = now - timedelta(seconds=self.settings.inspection_max_age_s + 10)
        self._seen = {key: entry for key, entry in self._seen.items() if entry[0] >= cutoff}
        self._bottles = {key: at for key, at in self._bottles.items() if at >= cutoff}
        self._heads = {key: entry for key, entry in self._heads.items() if entry[1] >= cutoff}
        session, sequence_text = event.inspection_id.split(":")
        sequence = int(sequence_text, 16)
        if sequence == 0:
            self.last_decision["reason"] = "invalid_identity"
            return None
        previous = self._seen.get(event.inspection_id)
        if previous is not None:
            self.last_decision["reason"] = (
                "duplicate" if previous[1] == event else "identity_conflict"
            )
            return None
        head = self._heads.get(session)
        late = head is not None and sequence <= head[0]
        if late and head is not None and event.inspected_at > head[2]:
            self.last_decision["reason"] = "sequence_regression"
            return None
        if head is None and len(self._heads) >= self.capacity:
            self.last_decision["reason"] = "capacity"
            return None
        # Track observed sequence separately from admitted decisions. An observed
        # but refused bottle/capacity delivery is not a missing transport snapshot.
        gap = max(0, sequence - head[0] - 1) if head is not None else 0
        self._heads[session] = (
            (head[0], now, head[2])
            if late and head is not None
            else (sequence, now, event.inspected_at)
        )
        self.last_decision["late_sequence"] = late
        self.last_decision["missing_sequences"] = gap
        bottle_key = (session, event.bottle_id)
        if bottle_key in self._bottles:
            self.last_decision["reason"] = "bottle_duplicate"
            return None
        if len(self._seen) >= self.capacity:
            self.last_decision["reason"] = "capacity"
            return None  # Never evict a live identity to admit another event.
        self._seen[event.inspection_id] = (now, event)
        self._bottles[bottle_key] = now
        self.last_decision = {
            "accepted": True,
            "reason": "passed" if event.passed else "failed",
            "missing_sequences": gap,
            "late_sequence": late,
        }
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
