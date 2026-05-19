"""Unit tests for actuation-event-validator."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from eirvah_contracts.actuation import ActuationRequest, ValidationResult
from eirvah_contracts.ulid import generate_correlation_id


def _sample_request(
    value: float = 22.0,
    requester: str = "decision-agent-stub",
    topic: str = "uniza/zilina/factory1/line_a/bottler/setpoint_unit/setpoint_temperature",
) -> ActuationRequest:
    now = datetime.now(UTC)
    return ActuationRequest(
        correlation_id=generate_correlation_id(),
        requester=requester,
        target_uns_topic=topic,
        requested_value=value,
        value_type="double",
        reason="test",
        requested_at=now,
    )


def _write_policy(tmp_path: Path) -> Path:
    policy_file = tmp_path / "actuation-policy.yaml"
    policy_file.write_text(
        "policies:\n"
        "  - uns_topic: \"uniza/zilina/factory1/line_a/bottler/setpoint_unit/setpoint_temperature\"\n"
        "    allowed_range: [20.0, 30.0]\n"
        "    allowlist:\n"
        "      - decision-agent-stub\n"
    )
    return policy_file


def test_validate_approve(tmp_path: Path) -> None:
    from actuation_event_validator.service import load_policy, validate_request

    policies = load_policy(_write_policy(tmp_path))
    result = validate_request(_sample_request(value=22.0), policies)
    assert result.decision == "approve"
    assert result.reason is None


def test_validate_reject_out_of_range(tmp_path: Path) -> None:
    from actuation_event_validator.service import load_policy, validate_request

    policies = load_policy(_write_policy(tmp_path))
    result = validate_request(_sample_request(value=99.0), policies)
    assert result.decision == "reject"
    assert result.reason is not None
    assert "outside policy range" in result.reason


def test_validate_reject_unknown_requester(tmp_path: Path) -> None:
    from actuation_event_validator.service import load_policy, validate_request

    policies = load_policy(_write_policy(tmp_path))
    result = validate_request(_sample_request(requester="intruder"), policies)
    assert result.decision == "reject"
    assert result.reason is not None
    assert "allowlist" in result.reason


def test_validate_reject_unknown_topic(tmp_path: Path) -> None:
    from actuation_event_validator.service import load_policy, validate_request

    policies = load_policy(_write_policy(tmp_path))
    result = validate_request(
        _sample_request(topic="uniza/zilina/factory1/line_a/bottler/motor_01/rpm"),
        policies,
    )
    assert result.decision == "reject"
    assert result.reason is not None
    assert "no policy" in result.reason


def test_load_policy_from_yaml(tmp_path: Path) -> None:
    from actuation_event_validator.service import load_policy

    policies = load_policy(_write_policy(tmp_path))
    key = "uniza/zilina/factory1/line_a/bottler/setpoint_unit/setpoint_temperature"
    assert key in policies
    assert policies[key].allowed_range == (20.0, 30.0)
    assert "decision-agent-stub" in policies[key].allowlist
