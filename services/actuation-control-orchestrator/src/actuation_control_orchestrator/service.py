"""Actuation control orchestrator service (spec §3.2)."""

from __future__ import annotations

import asyncio

import aio_pika
import structlog
import uvicorn
from eirvah_bus.client import BusClient
from eirvah_bus.consumer import subscribe_queue_group
from eirvah_contracts.envelope import NATSEnvelope
from eirvah_observability.health import HealthApp
from eirvah_observability.logging import configure_logging
from nats.aio.msg import Msg

from actuation_control_orchestrator.config import ActuationOrchestratorSettings
from actuation_control_orchestrator.metrics import ActuationMetrics
from actuation_control_orchestrator.models import ActuationPipelineConfig, load_pipeline_config
from actuation_control_orchestrator.pipeline import run_actuation_pipeline

_log = structlog.get_logger("actuation-control-orchestrator")
INGRESS_SUBJECT = "act.ingress.requested"


class ActuationOrchestratorRuntime:
    def __init__(self, settings: ActuationOrchestratorSettings) -> None:
        self._settings = settings
        self._bus: BusClient | None = None
        self._cfg: ActuationPipelineConfig | None = None
        self._amqp_exchange: aio_pika.abc.AbstractExchange | None = None
        self._metrics = ActuationMetrics()
        self._ready = False

    def is_ready(self) -> bool:
        return self._ready

    async def run(self) -> None:
        self._cfg = load_pipeline_config(self._settings.pipeline_config_path)
        self._bus = BusClient(servers=self._settings.nats_servers, name="actuation-control-orchestrator")
        await self._bus.connect()

        amqp_conn = await aio_pika.connect_robust(self._settings.amqp_url)
        amqp_channel = await amqp_conn.channel()
        self._amqp_exchange = await amqp_channel.declare_exchange(
            self._settings.amqp_results_exchange,
            aio_pika.ExchangeType.TOPIC,
            durable=True,
        )

        await subscribe_queue_group(
            nc=self._bus.nc, subject=INGRESS_SUBJECT, handler=self._handle
        )
        self._ready = True
        _log.info(
            "actuation_orchestrator_ready",
            subject=INGRESS_SUBJECT,
            allow_writes=self._settings.allow_writes,
        )
        await asyncio.get_event_loop().create_future()

    async def _handle(self, msg: Msg) -> None:
        assert self._cfg is not None and self._bus is not None and self._amqp_exchange is not None
        try:
            envelope = NATSEnvelope.model_validate_json(msg.data)
        except Exception as exc:
            _log.warning("invalid_ingress_message", error=str(exc))
            return
        await run_actuation_pipeline(
            envelope=envelope,
            cfg=self._cfg,
            nc=self._bus.nc,
            amqp_results_exchange=self._amqp_exchange,
            metrics=self._metrics,
            allow_writes=self._settings.allow_writes,
        )


async def run(settings: ActuationOrchestratorSettings) -> None:
    configure_logging(level=settings.log_level)
    runtime = ActuationOrchestratorRuntime(settings)
    health = HealthApp(is_ready=runtime.is_ready)
    http_cfg = uvicorn.Config(
        health.asgi,
        host="0.0.0.0",
        port=settings.http_port,
        log_level=settings.log_level.lower(),
    )
    http = uvicorn.Server(http_cfg)
    await asyncio.gather(runtime.run(), http.serve())
