"""UNS auto-contextualizer NATS req/rep worker — ontology-driven (SSN/SOSA)."""

from __future__ import annotations

import asyncio
from pathlib import Path

import structlog
import uvicorn
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
from rdflib import Graph
from rdflib.term import Literal

from uns_auto_contextualizer.config import AutoContextualizerSettings

_log = structlog.get_logger("uns-auto-contextualizer")
SUBJECT = "uns.work.contextualize"

_SPARQL = """
PREFIX sosa: <http://www.w3.org/ns/sosa/>
PREFIX ssn:  <http://www.w3.org/ns/ssn/>
PREFIX eirvah: <https://eirvah.uniza/ontology/>

SELECT ?sensor ?feature ?property
       ?enterprise ?site ?area ?line ?cell ?equipment ?measurement
       ?unit ?semanticType
WHERE {
  ?sensor eirvah:nodeId ?nodeId .
  ?sensor sosa:isHostedBy ?feature .
  ?sensor sosa:observes ?property .
  ?sensor eirvah:equipment ?equipment .
  ?sensor eirvah:measurement ?measurement .
  ?feature eirvah:enterprise ?enterprise .
  ?feature eirvah:site ?site .
  ?feature eirvah:area ?area .
  ?feature eirvah:line ?line .
  ?feature eirvah:cell ?cell .
  ?property eirvah:unit ?unit .
  ?property eirvah:semanticType ?semanticType .
}
"""


def load_ontology(path: Path) -> Graph:
    g = Graph()
    g.parse(str(path), format="json-ld")
    return g


def contextualize(
    normalized: NormalizedSignalEnvelope,
    graph: Graph,
) -> ContextualizeResult | None:
    results = list(graph.query(_SPARQL, initBindings={"nodeId": Literal(normalized.node_id)}))
    if not results:
        return None
    row = results[0]
    path = UNSPath(
        enterprise=str(row.enterprise),
        site=str(row.site),
        area=str(row.area),
        line=str(row.line),
        cell=str(row.cell),
        equipment=str(row.equipment),
        measurement=str(row.measurement),
    )
    return ContextualizeResult(
        uns_topic=build_uns_topic(path),
        uns_path=path,
        semantic_type=str(row.semanticType),
        sensor_uri=str(row.sensor),
        feature_uri=str(row.feature),
        property_uri=str(row.property),
    )


def handle_contextualize_request(
    envelope: NATSEnvelope,
    graph: Graph,
) -> NATSEnvelope:
    try:
        normalized = NormalizedSignalEnvelope.model_validate(envelope.payload)
        result = contextualize(normalized, graph)
        if result is None:
            return NATSEnvelope(
                correlation_id=envelope.correlation_id,
                status="error",
                error=EnvelopeError(
                    kind="UnknownNode",
                    message=f"no ontology entry for node_id {normalized.node_id!r}",
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
        self._graph: Graph | None = None
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
        self._graph = load_ontology(self._settings.ontology_path)
        node_count = sum(1 for _ in self._graph.subjects())
        self._bus = BusClient(
            servers=self._settings.nats_servers,
            name="uns-auto-contextualizer",
        )
        await self._bus.connect()
        await subscribe_queue_group(nc=self._bus.nc, subject=SUBJECT, handler=self._handle)
        self._ready = True
        _log.info("contextualizer_ready", subject=SUBJECT, ontology_nodes=node_count)
        await asyncio.get_event_loop().create_future()

    async def _handle(self, msg: Msg) -> None:
        try:
            envelope = NATSEnvelope.model_validate_json(msg.data)
        except Exception as exc:
            _log.warning("invalid_envelope", error=str(exc))
            return
        assert self._graph is not None
        reply = handle_contextualize_request(envelope, self._graph)
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
