from __future__ import annotations

from datetime import UTC, datetime, timedelta

from decision_agent_stub.service import TriggerWindow


def test_threshold_not_breached_returns_none() -> None:
    window = TriggerWindow(threshold=26.0, duration_s=30.0)
    now = datetime.now(UTC)
    result = window.update(value=25.9, ts=now, correlation_id="01HZXC8P9G7Q3M6V0K2T8R5W4A")
    assert result is None


def test_threshold_sustained_returns_request() -> None:
    window = TriggerWindow(threshold=26.0, duration_s=1.0, setpoint_target=22.0)
    now = datetime.now(UTC)
    window.update(value=27.0, ts=now, correlation_id="01HZXC8P9G7Q3M6V0K2T8R5W4A")
    later = now + timedelta(seconds=1.1)
    result = window.update(value=27.5, ts=later, correlation_id="01HZXC8P9G7Q3M6V0K2T8R5W4B")
    assert result is not None
    assert result.requested_value == 22.0
    assert result.requester == "decision-agent-stub"


def test_cooldown_prevents_second_fire() -> None:
    window = TriggerWindow(threshold=26.0, duration_s=1.0, cooldown_s=60.0)
    now = datetime.now(UTC)
    window.update(value=27.0, ts=now, correlation_id="01HZXC8P9G7Q3M6V0K2T8R5W4A")
    later = now + timedelta(seconds=1.1)
    first = window.update(value=27.0, ts=later, correlation_id="01HZXC8P9G7Q3M6V0K2T8R5W4B")
    assert first is not None
    # Immediately try again — cooldown should block it
    even_later = later + timedelta(seconds=0.1)
    second = window.update(value=27.0, ts=even_later, correlation_id="01HZXC8P9G7Q3M6V0K2T8R5W4C")
    assert second is None


def test_value_below_threshold_resets_window() -> None:
    window = TriggerWindow(threshold=26.0, duration_s=30.0)
    now = datetime.now(UTC)
    window.update(value=27.0, ts=now, correlation_id="01HZXC8P9G7Q3M6V0K2T8R5W4A")
    below_ts = now + timedelta(seconds=5)
    window.update(value=25.0, ts=below_ts, correlation_id="01HZXC8P9G7Q3M6V0K2T8R5W4B")
    # Window reset — even after original duration, needs to restart
    late_ts = now + timedelta(seconds=31)
    result = window.update(value=27.0, ts=late_ts, correlation_id="01HZXC8P9G7Q3M6V0K2T8R5W4C")
    assert result is None  # breach_start was reset at t+5, only 26s since reset
