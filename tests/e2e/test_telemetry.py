"""E2E tests for the telemetry path (spec §8.3 tests 1–2)."""

from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING, Any

import pytest
from eirvah_contracts.telemetry import TelemetryPayload
from eirvah_contracts.ulid import is_valid_correlation_id

if TYPE_CHECKING:
    from tests.e2e.conftest import EirVahCluster

pytestmark = pytest.mark.asyncio

SUBSCRIBE_TOPIC = "uniza/zilina/factory1/line_a/bottler/#"
EXPECTED_TOPICS = {
    "uniza/zilina/factory1/line_a/bottler/temperature_sensor_01/temperature",
    "uniza/zilina/factory1/line_a/bottler/throughput_meter_01/throughput",
    "uniza/zilina/factory1/line_a/bottler/motor_01/state",
    "uniza/zilina/factory1/line_a/bottler/motor_01/rpm",
    "uniza/zilina/factory1/line_a/bottler/setpoint_unit/setpoint_temperature",
}


async def _collect_messages(
    cluster: EirVahCluster,
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


async def test_telemetry_happy_path(eirvah_cluster: EirVahCluster) -> None:
    """spec §8.3 test 1: within 15 s at least one v1.0 message per monitored node."""
    messages = await _collect_messages(eirvah_cluster, timeout_s=15.0, max_messages=30)

    assert messages, (
        "No MQTT messages received within 15 s — pipeline may not be running"
    )

    topics_seen = {m["_topic"] for m in messages}
    missing = EXPECTED_TOPICS - topics_seen
    assert not missing, f"Missing messages for nodes: {missing}"

    for msg in messages:
        assert msg.get("schema_version") == "1.0", f"Bad schema_version in {msg}"
        assert is_valid_correlation_id(
            msg.get("correlation_id", "")
        ), f"Invalid correlation_id in {msg}"
        assert msg.get("quality") in {"good", "uncertain", "bad"}, (
            f"Invalid quality in {msg}"
        )
        TelemetryPayload.model_validate(msg)


async def test_quality_propagation(eirvah_cluster: EirVahCluster) -> None:
    """spec §8.3 test 2: ~10% of temperature messages carry quality='bad'."""
    messages = await _collect_messages(
        eirvah_cluster, timeout_s=20.0, max_messages=100
    )

    temp_topic = (
        "uniza/zilina/factory1/line_a/bottler/temperature_sensor_01/temperature"
    )
    temp_msgs = [m for m in messages if m.get("_topic") == temp_topic]
    assert len(temp_msgs) >= 5, (
        f"Need at least 5 temperature messages to assess quality, got {len(temp_msgs)}"
    )

    bad_count = sum(1 for m in temp_msgs if m.get("quality") == "bad")
    bad_pct = bad_count / len(temp_msgs)

    assert bad_pct > 0.02, (
        f"Expected some bad-quality temperature messages (bad_quality_pct=0.1 "
        f"in address-space config), got {bad_count}/{len(temp_msgs)}"
    )
