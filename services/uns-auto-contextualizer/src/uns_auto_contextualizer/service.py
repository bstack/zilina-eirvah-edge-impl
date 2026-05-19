"""UNS auto-contextualizer NATS req/rep worker (spec §3.1)."""

from __future__ import annotations

import asyncio
from pathlib import Path

import structlog
import uvicorn
import yaml
from eirvah_bus.client import BusClient
from eirvah_bus.consumer import subscribe_queue_group
from eirvah_contracts.envelope import EnvelopeError, NATSEnvelope
from eirvah_contracts.pipeline import ContextualizeResult
from eirvah_contracts.signals import NormalizedSignalEnvelope
from eirvah_contracts.uns import UNSPath, build_uns_topic
from eirvah_observability.health import HealthApp
from eirvah_observability.logging import configure_logging
from eirvah_observability.metrics import make_counter
from nats.aio.msg import Msg
from pydantic import BaseModel

from uns_auto_contextualizer.config import AutoContextualizerSettings

_log = structlog.get_logger("uns-auto-contextualizer")
SUBJECT = "uns.work.contextualize"


class MappingEntry(BaseModel):
    node_id: str
    area: str
    line: str
    cell: str
    equipment: str
    measurement: str
    semantic_type: str


def load_mapping(path: Path) -> dict[str, MappingEntry]:
    raw = yaml.safe_load(path.read_text())
    return {m["node_id"]: MappingEntry.model_validate(m) for m in raw["mappings"]}


def contextualize(
    normalized: NormalizedSignalEnvelope,
    mapping: dict[str, MappingEntry],
    *,
    enterprise: str,
    site: str,
) -> ContextualizeResult | None:
    entry = mapping.get(normalized.node_id)
    if entry is None:
        return None
    path = UNSPath(
        enterprise=enterprise,
        site=site,
        area=entry.area,
        line=entry.line,
        cell=entry.cell,
        equipment=entry.equipment,
        measurement=entry.measurement,
    )
    return ContextualizeResult(
        uns_topic=build_uns_topic(path),
        uns_path=path,
        semantic_type=entry.semantic_type,
    )


def handle_contextualize_request(
    envelope: NATSEnvelope,
    mapping: dict[str, MappingEntry],
    *,
    enterprise: str,
    site: str,
) -> NATSEnvelope:
    try:
        normalized = NormalizedSignalEnvelope.model_validate(envelope.payload)
        result = contextualize(normalized, mapping, enterprise=enterprise, site=site)
        if result is None:
            return NATSEnvelope(
                correlation_id=envelope.correlation_id,
                status="error",
                error=EnvelopeError(
                    kind="UnknownNode",
                    message=f"no mapping for node_id {normalized.node_id!r}",
                ),
            )
        return NATSEnvelope(
            correlation_id=envelope.correlation_id,
            payload=result.model_dump(mode="json"),
        )
    except Exception as exc:
        return NATSEnvelope(
            correlation_id=envelope.correlation_id,
            status="error",
            error=EnvelopeError(kind=type(exc).__name__, message=str(exc)[:200]),
        )


class AutoContextualizerWorker:
    def __init__(self, settings: AutoContextualizerSettings) -> None:
        self._settings = settings
        self._mapping: dict[str, MappingEntry] = {}
        self._bus: BusClient | None = None
        self._ready = False
        self._handled = make_counter(
            "worker_handler_total",
            "Worker handler invocations",
            labelnames=["worker", "outcome"],
        )

    def is_ready(self) -> bool:
        return self._ready

    async def run(self) -> None:
        self._mapping = load_mapping(self._settings.mapping_path)
        self._bus = BusClient(
            servers=self._settings.nats_servers,
            name="uns-auto-contextualizer",
        )
        await self._bus.connect()
        await subscribe_queue_group(nc=self._bus.nc, subject=SUBJECT, handler=self._handle)
        self._ready = True
        _log.info("contextualizer_ready", subject=SUBJECT, mappings=len(self._mapping))
        await asyncio.get_event_loop().create_future()

    async def _handle(self, msg: Msg) -> None:
        try:
            envelope = NATSEnvelope.model_validate_json(msg.data)
        except Exception as exc:
            _log.warning("invalid_envelope", error=str(exc))
            return
        reply = handle_contextualize_request(
            envelope,
            self._mapping,
            enterprise=self._settings.enterprise,
            site=self._settings.site,
        )
        self._handled.labels(worker="uns-auto-contextualizer", outcome=reply.status).inc()
        await msg.respond(reply.model_dump_json().encode())


async def run(settings: AutoContextualizerSettings) -> None:
    configure_logging(level=settings.log_level)
    worker = AutoContextualizerWorker(settings)
    health = HealthApp(is_ready=worker.is_ready)
    http_cfg = uvicorn.Config(
        health.asgi,
        host="0.0.0.0",
        port=settings.http_port,
        log_level=settings.log_level.lower(),
    )
    http = uvicorn.Server(http_cfg)
    await asyncio.gather(worker.run(), http.serve())
