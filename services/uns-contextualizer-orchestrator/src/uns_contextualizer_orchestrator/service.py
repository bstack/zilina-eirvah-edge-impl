"""Main service entry point for the UNS contextualizer orchestrator."""

from __future__ import annotations

import asyncio

import structlog
import uvicorn
from eirvah_bus.client import BusClient
from eirvah_bus.consumer import subscribe_queue_group
from eirvah_contracts.envelope import NATSEnvelope
from eirvah_observability.health import HealthApp
from eirvah_observability.logging import configure_logging
from nats.aio.msg import Msg

from uns_contextualizer_orchestrator.config import OrchestratorSettings
from uns_contextualizer_orchestrator.metrics import PipelineMetrics
from uns_contextualizer_orchestrator.models import PipelineConfig, load_pipeline_config
from uns_contextualizer_orchestrator.pipeline import run_pipeline

_log = structlog.get_logger("uns-contextualizer-orchestrator")

INGRESS_SUBJECT = "uns.ingress.raw"


class OrchestratorRuntime:
    def __init__(self, settings: OrchestratorSettings) -> None:
        self._settings = settings
        self._bus: BusClient | None = None
        self._cfg: PipelineConfig | None = None
        self._metrics = PipelineMetrics()
        self._ready = False

    def is_ready(self) -> bool:
        return self._ready

    async def run(self) -> None:
        self._cfg = load_pipeline_config(self._settings.pipeline_config_path)
        self._bus = BusClient(
            servers=self._settings.nats_servers,
            name="uns-contextualizer-orchestrator",
        )
        await self._bus.connect()
        await subscribe_queue_group(
            nc=self._bus.nc, subject=INGRESS_SUBJECT, handler=self._handle
        )
        self._ready = True
        _log.info(
            "orchestrator_ready",
            subject=INGRESS_SUBJECT,
            stages=[s.name for s in self._cfg.stages],
        )
        await asyncio.get_event_loop().create_future()

    async def _handle(self, msg: Msg) -> None:
        assert self._cfg is not None and self._bus is not None
        try:
            envelope = NATSEnvelope.model_validate_json(msg.data)
        except Exception as exc:
            _log.warning("invalid_ingress_message", error=str(exc))
            return
        await run_pipeline(
            envelope=envelope,
            cfg=self._cfg,
            nc=self._bus.nc,
            metrics=self._metrics,
        )


async def run(settings: OrchestratorSettings) -> None:
    configure_logging(level=settings.log_level)
    runtime = OrchestratorRuntime(settings)
    health = HealthApp(is_ready=runtime.is_ready)
    http_cfg = uvicorn.Config(
        health.asgi,
        host="0.0.0.0",
        port=settings.http_port,
        log_level=settings.log_level.lower(),
    )
    http = uvicorn.Server(http_cfg)
    await asyncio.gather(runtime.run(), http.serve())
