"""E2E tests for the telemetry path — SSN/SOSA payload validation."""

from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING, Any

import pytest
from eirvah_contracts.sosa import SOSAObservation
from eirvah_contracts.ulid import is_valid_correlation_id

if TYPE_CHECKING:
    from tests.e2e.conftest import EirVahCluster

pytestmark = pytest.mark.asyncio

SUBSCRIBE_TOPIC = "uniza/zilina/factory1/line_a/bottler/#"
# High-frequency topics (change every tick — reliable in a 15s window)
EXPECTED_TOPICS = {
    "uniza/zilina/factory1/line_a/bottler/temperature_sensor_01/temperature",
    "uniza/zilina/factory1/line_a/bottler/throughput_meter_01/throughput",
    "uniza/zilina/factory1/line_a/bottler/motor_01/rpm",
}
# motor_01/state and setpoint_temperature only publish on value change (OPC UA deadband).
# They appear when a disturbance runs; excluded from the always-green smoke test.


async def _collect_messages(
    cluster: "EirVahCluster",
    *,
    timeout_s: float = 15.0,
    max_messages: int = 50,
) -> list[dict[str, Any]]:
    messages: list[dict[str, Any]] = []
    async with cluster.mqtt_client() as client:
        await client.subscribe(SUBSCRIBE_TOPIC, qos=1)
        try:
            async with asyncio.timeout(timeout_s):
                async for msg in client.messages:
                    payload = json.loads(msg.payload)
                    payload["_topic"] = str(msg.topic)
                    messages.append(payload)
                    if len(messages) >= max_messages:
                        break
        except TimeoutError:
            pass
    return messages


async def test_telemetry_happy_path(eirvah_cluster: "EirVahCluster") -> None:
    """All bottler nodes publish sosa:Observation within 15 s."""
    messages = await _collect_messages(eirvah_cluster, timeout_s=15.0, max_messages=100)

    assert messages, "No MQTT messages received within 15 s — pipeline may not be running"

    topics_seen = {m["_topic"] for m in messages}
    missing = EXPECTED_TOPICS - topics_seen
    assert not missing, f"Missing messages for topics: {missing}"

    for msg in messages:
        assert msg.get("@type") == "sosa:Observation", (
            f"Expected sosa:Observation, got {msg.get('@type')!r}"
        )
        assert "@context" in msg, "Missing @context"
        assert "sosa:hasSimpleResult" in msg, "Missing sosa:hasSimpleResult"
        assert "sosa:madeBySensor" in msg, "Missing sosa:madeBySensor"
        assert is_valid_correlation_id(msg.get("eirvah:correlationId", "")), (
            f"Invalid correlationId in {msg}"
        )
        assert msg.get("eirvah:quality") in {"good", "uncertain", "bad"}, (
            f"Invalid quality in {msg}"
        )
        obs = SOSAObservation.from_jsonld(msg)
        assert obs.get_value() is not None


async def test_quality_field_present(eirvah_cluster: "EirVahCluster") -> None:
    """Every sosa:Observation carries a valid eirvah:quality field."""
    messages = await _collect_messages(eirvah_cluster, timeout_s=10.0, max_messages=20)

    temp_topic = "uniza/zilina/factory1/line_a/bottler/temperature_sensor_01/temperature"
    temp_msgs = [m for m in messages if m.get("_topic") == temp_topic]
    assert len(temp_msgs) >= 3, (
        f"Need at least 3 temperature messages, got {len(temp_msgs)}"
    )

    for msg in temp_msgs:
        assert msg.get("eirvah:quality") in {"good", "uncertain", "bad"}, (
            f"Invalid eirvah:quality in {msg}"
        )
