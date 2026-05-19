"""Actuation signal publisher NATS req/rep worker (spec §3.2)."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import structlog
import uvicorn
import yaml
from asyncua import Client
from eirvah_bus.client import BusClient
from eirvah_bus.consumer import subscribe_queue_group
from eirvah_contracts.actuation import ActuationRequest
from eirvah_contracts.envelope import EnvelopeError, NATSEnvelope
from eirvah_observability.health import HealthApp
from eirvah_observability.logging import configure_logging
from eirvah_observability.metrics import make_counter
from nats.aio.msg import Msg
from pydantic import BaseModel

from actuation_signal_publisher.config import SignalPublisherSettings

_log = structlog.get_logger("actuation-signal-publisher")
SUBJECT = "act.work.write_signal"


@dataclass
class WriteTarget:
    browse_names: list[str]
    endpoint: str
    namespace_uri: str


class _NodeListEntry(BaseModel):
    browse_names: list[str]
    alias: str


class _NodeListConfig(BaseModel):
    endpoint: str
    namespace_uri: str
    publishing_interval_ms: int = 500
    nodes: list[_NodeListEntry]


def load_write_targets(
    *,
    mapping_path: Path,
    node_list_path: Path,
    enterprise: str,
    site: str,
) -> dict[str, WriteTarget]:
    """Build uns_topic → WriteTarget. Fails if mapping is not bijective."""
    mapping_raw = yaml.safe_load(mapping_path.read_text())
    node_list_raw = yaml.safe_load(node_list_path.read_text())
    node_list = _NodeListConfig.model_validate(node_list_raw)

    alias_to_browse: dict[str, list[str]] = {
        entry.alias: entry.browse_names for entry in node_list.nodes
    }

    targets: dict[str, WriteTarget] = {}
    for entry in mapping_raw["mappings"]:
        alias = entry["node_id"]
        topic = (
            f"{enterprise}/{site}/{entry['area']}/{entry['line']}"
            f"/{entry['cell']}/{entry['equipment']}/{entry['measurement']}"
        )
        if topic in targets:
            raise ValueError(
                f"mapping not bijective: topic {topic!r} maps to multiple node_ids"
            )
        browse_names = alias_to_browse.get(alias)
        if browse_names is None:
            continue  # node not in node-list — not writable via this service
        targets[topic] = WriteTarget(
            browse_names=browse_names,
            endpoint=node_list.endpoint,
            namespace_uri=node_list.namespace_uri,
        )

    return targets


async def write_opcua_value(
    *,
    target: WriteTarget,
    value: Any,
) -> None:
    async with Client(url=target.endpoint) as client:
        ns_idx = await client.get_namespace_index(target.namespace_uri)
        path = [f"{ns_idx}:{name}" for name in target.browse_names]
        node = await client.nodes.objects.get_child(path)
        await node.write_value(value)


class SignalPublisherWorker:
    def __init__(self, settings: SignalPublisherSettings) -> None:
        self._settings = settings
        self._targets: dict[str, WriteTarget] = {}
        self._ready = False
        self._handled = make_counter(
            "worker_handler_total",
            "Worker handler invocations",
            labelnames=["worker", "outcome"],
        )

    def is_ready(self) -> bool:
        return self._ready

    async def run(self) -> None:
        self._targets = load_write_targets(
            mapping_path=self._settings.mapping_path,
            node_list_path=self._settings.node_list_path,
            enterprise=self._settings.enterprise,
            site=self._settings.site,
        )
        bus = BusClient(servers=self._settings.nats_servers, name="actuation-signal-publisher")
        await bus.connect()
        await subscribe_queue_group(nc=bus.nc, subject=SUBJECT, handler=self._handle)
        self._ready = True
        _log.info("signal_publisher_ready", subject=SUBJECT, targets=len(self._targets))
        await asyncio.get_event_loop().create_future()

    async def _handle(self, msg: Msg) -> None:
        correlation_id = "UNKNOWN"
        try:
            envelope = NATSEnvelope.model_validate_json(msg.data)
            correlation_id = envelope.correlation_id
            req = ActuationRequest.model_validate(envelope.payload)

            target = self._targets.get(req.target_uns_topic)
            if target is None:
                raise ValueError(f"no write target for topic {req.target_uns_topic!r}")

            await write_opcua_value(target=target, value=req.requested_value)

            self._handled.labels(worker="actuation-signal-publisher", outcome="ok").inc()
            reply = NATSEnvelope(correlation_id=correlation_id)
        except Exception as exc:
            self._handled.labels(worker="actuation-signal-publisher", outcome="error").inc()
            reply = NATSEnvelope(
                correlation_id=correlation_id,
                status="error",
                error=EnvelopeError(kind=type(exc).__name__, message=str(exc)[:200]),
            )
        await msg.respond(reply.model_dump_json().encode())


async def run(settings: SignalPublisherSettings) -> None:
    configure_logging(level=settings.log_level)
    worker = SignalPublisherWorker(settings)
    health = HealthApp(is_ready=worker.is_ready)
    http_cfg = uvicorn.Config(
        health.asgi,
        host="0.0.0.0",
        port=settings.http_port,
        log_level=settings.log_level.lower(),
    )
    http = uvicorn.Server(http_cfg)
    await asyncio.gather(worker.run(), http.serve())
