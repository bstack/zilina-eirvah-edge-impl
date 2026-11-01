from datetime import UTC, datetime, timedelta

import pytest
from decision_agent_stub.config import DecisionAgentSettings
from decision_agent_stub.service import ProductionModeRule
from eirvah_contracts.ulid import is_valid_correlation_id


def test_production_modes_emit_once_and_return_to_normal() -> None:
    rule = ProductionModeRule(DecisionAgentSettings())
    now = datetime.now(UTC)
    normal = rule.update(speed=0.4, observed_at=now, now=now)
    assert normal is not None and normal.requested_value == 22.0
    assert rule.update(speed=0.45, observed_at=now, now=now) is None
    high = rule.update(speed=0.7, observed_at=now, now=now)
    assert high is not None and high.requested_value == 26.0
    assert is_valid_correlation_id(high.correlation_id)
    assert high.correlation_id != normal.correlation_id
    assert rule.update(speed=0.7, observed_at=now, now=now) is None
    assert rule.update(speed=0.55, observed_at=now, now=now) is None
    normal_again = rule.update(speed=0.4, observed_at=now, now=now)
    assert normal_again is not None and normal_again.requested_value == 22.0


@pytest.mark.parametrize("speed", [True, -0.1, float("nan"), float("inf"), 1.0])
def test_invalid_speed_cannot_select_mode(speed: float) -> None:
    now = datetime.now(UTC)
    assert (
        ProductionModeRule(DecisionAgentSettings()).update(speed=speed, observed_at=now, now=now)
        is None
    )


def test_stale_future_out_of_order_and_bad_quality_are_ignored() -> None:
    rule = ProductionModeRule(DecisionAgentSettings())
    now = datetime.now(UTC)
    for ts in (now - timedelta(seconds=20), now + timedelta(seconds=20)):
        assert rule.update(speed=0.7, observed_at=ts, now=now) is None
    assert rule.update(speed=0.7, observed_at=now, now=now, quality="bad") is None
    assert rule.update(speed=0.7, observed_at=now, now=now) is not None
    assert rule.update(speed=0.4, observed_at=now - timedelta(seconds=1), now=now) is None


def test_disabled_rule_does_not_override_manual_temperature() -> None:
    now = datetime.now(UTC)
    assert (
        ProductionModeRule(DecisionAgentSettings(enabled=False)).update(
            speed=0.7, observed_at=now, now=now
        )
        is None
    )
