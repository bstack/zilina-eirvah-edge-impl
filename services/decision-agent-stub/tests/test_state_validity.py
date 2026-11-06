from datetime import UTC, datetime, timedelta

from decision_agent_stub.config import DecisionAgentSettings
from decision_agent_stub.service import DecisionAgentRuntime as Runtime
from prometheus_client.registry import CollectorRegistry


def DecisionAgentRuntime(settings):
    return Runtime(settings, registry=CollectorRegistry())


def observation(at, value, quality="good"):
    return {
        "sosa:resultTime": at.isoformat(),
        "sosa:hasSimpleResult": value,
        "eirvah:unit": "m/s",
        "eirvah:quality": quality,
    }


def test_same_timestamp_conflict_cannot_change_the_selected_mode():
    runtime = DecisionAgentRuntime(DecisionAgentSettings())
    now = datetime.now(UTC)
    topic = runtime._settings.subscribe_topic
    first = runtime.observation(topic, observation(now, 0.4), now)

    conflict = runtime.observation(topic, observation(now, 0.7), now)

    assert first is not None and first.requested_value == 22
    assert conflict is None
    assert runtime.state_decision["reason"] == "timestamp_conflict"
    assert runtime.state_snapshot(now)[topic]["value"] == 0.4


def test_missing_state_becomes_stale_and_consumer_restart_starts_unknown():
    settings = DecisionAgentSettings()
    runtime = DecisionAgentRuntime(settings)
    now = datetime.now(UTC)
    runtime.observation(settings.subscribe_topic, observation(now, 0.4), now)

    stale = runtime.state_snapshot(now + timedelta(seconds=6))
    restarted = DecisionAgentRuntime(settings).state_snapshot(now + timedelta(seconds=6))

    assert stale[settings.subscribe_topic]["status"] == "stale"
    assert restarted[settings.subscribe_topic]["status"] == "unknown"


def test_old_or_invalid_state_does_not_overwrite_fresh_feedback():
    runtime = DecisionAgentRuntime(DecisionAgentSettings())
    now = datetime.now(UTC)
    topic = runtime._settings.subscribe_topic
    runtime.observation(topic, observation(now, 0.7), now)

    for at, quality in [
        (now - timedelta(seconds=1), "good"),
        (now + timedelta(seconds=1), "good"),
        (now, "bad"),
    ]:
        assert runtime.observation(topic, observation(at, 0.4, quality), now) is None

    assert runtime.state_snapshot(now)[topic]["value"] == 0.7
