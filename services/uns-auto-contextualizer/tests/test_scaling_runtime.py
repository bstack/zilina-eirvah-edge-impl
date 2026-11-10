"""Scaling keeps queue-group routing single delivery and health scheduling responsive."""

import asyncio
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from eirvah_contracts.envelope import NATSEnvelope
from load_inject import NODE_IDS, _build_payload
from nats.aio.msg import Msg
from prometheus_client import CollectorRegistry
from uns_auto_contextualizer.config import AutoContextualizerSettings
from uns_auto_contextualizer.service import AutoContextualizerWorker, load_ontology


async def test_cpu_handler_does_not_block_health_loop_and_replies_once():
    registry = CollectorRegistry()
    worker = AutoContextualizerWorker(AutoContextualizerSettings(), registry=registry)
    worker._graph = load_ontology(Path("config/eirvah-line-a.jsonld"))
    request = NATSEnvelope.model_validate_json(_build_payload(NODE_IDS[0]))
    client = SimpleNamespace(publish=AsyncMock())
    message = Msg(
        _client=client,
        reply="inbox",
        data=request.model_dump_json().encode(),
        headers={"X-Correlation-Id": request.correlation_id},
    )
    from uns_auto_contextualizer import service

    actual = service.handle_contextualize_request
    ticks = []

    def slow(envelope, graph):
        time.sleep(0.08)
        return actual(envelope, graph)

    async def health_tick():
        await asyncio.sleep(0.02)
        ticks.append(not client.publish.called)

    with patch.object(service, "handle_contextualize_request", side_effect=slow):
        await asyncio.gather(worker._handle(message), health_tick())

    assert ticks == [True]
    client.publish.assert_awaited_once()
    args = client.publish.call_args
    assert NATSEnvelope.model_validate_json(args.args[1]).correlation_id == request.correlation_id
    assert args.kwargs["headers"]["X-Eirvah-Worker"]
    assert args.kwargs["headers"]["X-Correlation-Id"] == request.correlation_id
    assert (
        registry.get_sample_value("eirvah_worker_inflight", {"worker": "uns-auto-contextualizer"})
        == 0
    )
    assert (
        registry.get_sample_value(
            "eirvah_worker_handler_total", {"worker": "uns-auto-contextualizer", "outcome": "ok"}
        )
        == 1
    )
    assert (
        registry.get_sample_value(
            "eirvah_worker_service_seconds_count", {"worker": "uns-auto-contextualizer"}
        )
        == 1
    )
