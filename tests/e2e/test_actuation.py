"""E2E tests for the actuation path (spec §8.4, Plan 3 design §6)."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import aio_pika
import pytest
from eirvah_contracts.actuation import ActuationRequest
from eirvah_contracts.ulid import generate_correlation_id

if TYPE_CHECKING:
    from tests.e2e.conftest import EirVahCluster

pytestmark = pytest.mark.asyncio

AMQP_RESULTS_EXCHANGE = "eirvah.actuation.results"
AMQP_REQUESTS_QUEUE = "eirvah.actuation.requests"
SETPOINT_TOPIC = (
    "uniza/zilina/factory1/line_a/bottler/setpoint_unit/setpoint_temperature"
)
_VALID_REQUEST_VALUE = 22.0
_OUT_OF_RANGE_VALUE = 99.0


def _build_request(
    *,
    value: float = _VALID_REQUEST_VALUE,
    requester: str = "decision-agent-stub",
    deadline_offset_s: float = 10.0,
) -> ActuationRequest:
    now = datetime.now(UTC)
    return ActuationRequest(
        correlation_id=generate_correlation_id(),
        requester=requester,
        target_uns_topic=SETPOINT_TOPIC,
        requested_value=value,
        value_type="double",
        reason="e2e test",
        requested_at=now,
        deadline=now + timedelta(seconds=deadline_offset_s),
    )


async def _publish_request(amqp_url: str, req: ActuationRequest) -> None:
    connection = await aio_pika.connect_robust(amqp_url)
    async with connection:
        channel = await connection.channel()
        await channel.default_exchange.publish(
            aio_pika.Message(
                body=req.model_dump_json().encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key=AMQP_REQUESTS_QUEUE,
        )


async def _consume_result(
    amqp_url: str,
    *,
    timeout_s: float = 15.0,
) -> dict[str, Any]:
    """Bind a temp queue to the results exchange, return first message."""
    connection = await aio_pika.connect_robust(amqp_url)
    async with connection:
        channel = await connection.channel()
        exchange = await channel.declare_exchange(
            AMQP_RESULTS_EXCHANGE,
            aio_pika.ExchangeType.TOPIC,
            durable=True,
        )
        result_queue = await channel.declare_queue("", exclusive=True)
        await result_queue.bind(exchange, routing_key="#")
        try:
            async with asyncio.timeout(timeout_s):
                async with result_queue.iterator() as q:
                    async for message in q:
                        async with message.process():
                            return json.loads(message.body)
        except TimeoutError:
            pytest.fail(f"No AMQP result received within {timeout_s}s")
    return {}


async def _read_opcua_setpoint(cluster: "EirVahCluster") -> float:
    """Read current setpoint value from OPC UA simulator."""
    from asyncua import Client

    async with Client(url=cluster.opcua_endpoint) as client:
        ns_idx = await client.get_namespace_index(
            "https://eirvah.uniza/zilina/factory1"
        )
        node = await client.nodes.objects.get_child(
            [f"{ns_idx}:bottler", f"{ns_idx}:SetpointTemperature"]
        )
        return float(await node.read_value())


async def test_actuation_full_loop(eirvah_cluster: "EirVahCluster") -> None:
    """Full CPS loop: request → approve → OPC UA write → setpoint changes."""
    req = _build_request(value=_VALID_REQUEST_VALUE)
    await _publish_request(eirvah_cluster.amqp_url, req)
    result = await _consume_result(eirvah_cluster.amqp_url, timeout_s=15.0)

    assert result.get("decision") == "approve", (
        f"Expected approve, got: {result}"
    )
    assert result.get("correlation_id") == req.correlation_id

    await asyncio.sleep(1.0)
    setpoint = await _read_opcua_setpoint(eirvah_cluster)
    assert abs(setpoint - _VALID_REQUEST_VALUE) < 0.01, (
        f"Expected setpoint {_VALID_REQUEST_VALUE}, got {setpoint}"
    )


async def test_actuation_rejection_policy(eirvah_cluster: "EirVahCluster") -> None:
    """Value outside allowed_range [20.0, 30.0] → reject with policy reason."""
    req = _build_request(value=_OUT_OF_RANGE_VALUE)
    await _publish_request(eirvah_cluster.amqp_url, req)
    result = await _consume_result(eirvah_cluster.amqp_url, timeout_s=10.0)

    assert result.get("decision") == "reject", (
        f"Expected reject, got: {result}"
    )
    assert "outside policy range" in (result.get("rejection_reason") or ""), (
        f"Expected policy range reason, got: {result}"
    )


async def test_actuation_rejection_writes_disabled(
    eirvah_cluster: "EirVahCluster",
) -> None:
    """With allow_writes=false (default), any valid request → writes_disabled."""
    req = _build_request(value=_VALID_REQUEST_VALUE)
    await _publish_request(eirvah_cluster.amqp_url, req)
    result = await _consume_result(eirvah_cluster.amqp_url, timeout_s=10.0)

    assert result.get("decision") == "reject"
    assert result.get("rejection_reason") == "writes_disabled"


async def test_actuation_deadline_expired(
    eirvah_cluster: "EirVahCluster",
) -> None:
    """Request with past deadline → reject with reason 'expired'."""
    req = _build_request(value=_VALID_REQUEST_VALUE, deadline_offset_s=-5.0)
    await _publish_request(eirvah_cluster.amqp_url, req)
    result = await _consume_result(eirvah_cluster.amqp_url, timeout_s=10.0)

    assert result.get("decision") == "reject"
    assert result.get("rejection_reason") == "expired"
