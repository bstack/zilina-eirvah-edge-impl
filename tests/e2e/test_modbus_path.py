"""E2E test for the Modbus ingress path.

Verifies that modbus-data-subscriber publishes RawSignalEnvelope messages
onto uns.ingress.raw within a reasonable time window.

Requires a live cluster — skipped automatically if absent (via eirvah_cluster fixture).
"""
from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING, Any

import nats
import pytest

if TYPE_CHECKING:
    from tests.e2e.conftest import EirVahCluster

pytestmark = pytest.mark.asyncio

EXPECTED_ALIASES = {
    "Filler.FillLevelSensor01",
    "Filler.Motor01.State",
    "Filler.ThroughputMeter01",
    "Conveyor.Belt01.BeltSpeed",
    "Conveyor.Belt01.JamDetected",
    "Conveyor.Belt01.BottleCount",
    "RejectStation.RejectCounter01",
    "RejectStation.ConveyorActive01",
}


async def _collect_raw_messages(
    cluster: "EirVahCluster",
    *,
    timeout_s: float = 10.0,
    max_messages: int = 40,
) -> list[dict[str, Any]]:
    messages: list[dict[str, Any]] = []
    nc = await nats.connect(servers=cluster.nats_servers)
    try:
        sub = await nc.subscribe("uns.ingress.raw")
        try:
            async with asyncio.timeout(timeout_s):
                async for msg in sub.messages:
                    outer = json.loads(msg.data)
                    payload = outer.get("payload", {})
                    messages.append(payload)
                    if len(messages) >= max_messages:
                        break
        except TimeoutError:
            pass
        finally:
            await sub.unsubscribe()
    finally:
        await nc.close()
    return messages


async def test_modbus_path_publishes_all_aliases(eirvah_cluster: "EirVahCluster") -> None:
    messages = await _collect_raw_messages(eirvah_cluster, timeout_s=10.0, max_messages=40)
    assert messages, "No messages received on uns.ingress.raw — is modbus-data-subscriber running?"

    seen_aliases = {m["node_id"] for m in messages if "node_id" in m}
    modbus_aliases = seen_aliases & EXPECTED_ALIASES
    assert modbus_aliases == EXPECTED_ALIASES, (
        f"Missing Modbus aliases on uns.ingress.raw: {EXPECTED_ALIASES - modbus_aliases}"
    )


async def test_modbus_envelope_schema(eirvah_cluster: "EirVahCluster") -> None:
    from eirvah_contracts.signals import RawSignalEnvelope

    messages = await _collect_raw_messages(eirvah_cluster, timeout_s=10.0, max_messages=20)
    modbus_msgs = [m for m in messages if m.get("source_endpoint", "").startswith("modbus-tcp://")]
    assert modbus_msgs, "No Modbus messages found — check source_endpoint prefix"

    for raw in modbus_msgs[:5]:
        env = RawSignalEnvelope.model_validate(raw)
        assert env.quality == "good"
        assert env.source_endpoint.startswith("modbus-tcp://")
