import json
from pathlib import Path

import pytest
from eirvah_contracts.actuation import (
    ActuationApproveResult,
    ActuationRejectResult,
    ActuationRequest,
)

GOLDEN_DIR = Path(__file__).parent / "golden"


def _load(name: str) -> dict:
    return json.loads((GOLDEN_DIR / name).read_text())


def test_request_golden_validates() -> None:
    req = ActuationRequest.model_validate(_load("actuation_request_v1_0_sample.json"))
    assert req.requested_value == 22.0
    assert req.requester == "decision-agent-stub"


def test_request_round_trip_through_json() -> None:
    raw = _load("actuation_request_v1_0_sample.json")
    req = ActuationRequest.model_validate(raw)
    assert json.loads(req.model_dump_json()) == raw


def test_approve_result_golden_validates() -> None:
    res = ActuationApproveResult.model_validate(_load("actuation_approve_v1_0_sample.json"))
    assert res.decision == "approve"
    assert res.written_at is not None


def test_reject_result_golden_validates() -> None:
    res = ActuationRejectResult.model_validate(_load("actuation_reject_v1_0_sample.json"))
    assert res.decision == "reject"
    assert "outside policy range" in res.rejection_reason


def test_approve_rejects_decision_other_than_approve() -> None:
    raw = _load("actuation_approve_v1_0_sample.json")
    raw["decision"] = "reject"
    with pytest.raises(ValueError):
        ActuationApproveResult.model_validate(raw)


def test_reject_rejects_decision_other_than_reject() -> None:
    raw = _load("actuation_reject_v1_0_sample.json")
    raw["decision"] = "approve"
    with pytest.raises(ValueError):
        ActuationRejectResult.model_validate(raw)


def test_request_rejects_invalid_correlation_id() -> None:
    raw = _load("actuation_request_v1_0_sample.json")
    raw["correlation_id"] = "not-a-ulid"
    with pytest.raises(ValueError):
        ActuationRequest.model_validate(raw)


def test_request_rejects_malformed_target_uns_topic() -> None:
    raw = _load("actuation_request_v1_0_sample.json")
    raw["target_uns_topic"] = "too/few/segments"
    with pytest.raises(ValueError):
        ActuationRequest.model_validate(raw)
