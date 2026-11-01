from unittest.mock import AsyncMock, patch

import pytest
from disturbance import apply_cycle


async def test_cycle_uses_normal_command_path_and_independent_cooling() -> None:
    client = AsyncMock()
    client.get_namespace_index.return_value = 2
    bottler = AsyncMock()
    client.nodes.objects.get_child.return_value = bottler
    with patch("disturbance.send", new_callable=AsyncMock) as send:
        send.return_value = {"decision": "approve", "written_at": "2026-11-01T20:00:00Z"}
        await apply_cycle(client, speed=0.7, amqp_url="amqp://example", namespace_uri="urn:test")
    request = send.call_args.args[0]
    assert request.target_uns_topic.endswith("conveyor/belt_01/speed_setpoint")
    assert request.requested_value == 0.7
    bottler.call_method.assert_awaited_once_with("2:TriggerCoolingLoad")


async def test_rejected_speed_command_does_not_inject_cooling() -> None:
    client = AsyncMock()
    with (
        patch("disturbance.send", new_callable=AsyncMock, return_value={"decision": "reject"}),
        pytest.raises(RuntimeError, match="not confirmed"),
    ):
        await apply_cycle(client, speed=0.7, amqp_url="amqp://example", namespace_uri="urn:test")
    client.get_namespace_index.assert_not_awaited()


def test_harness_accepts_an_explicit_amqp_port() -> None:
    from harness import parse_args
    assert parse_args(["run", "--experiment", "a", "--amqp-port", "15673"]).amqp_port == 15673
