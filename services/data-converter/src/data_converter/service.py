"""Data-converter NATS req/rep worker (spec §3.1)."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import structlog
import uvicorn
import yaml
from eirvah_bus.client import BusClient
from eirvah_bus.consumer import subscribe_queue_group
from eirvah_contracts.envelope import EnvelopeError, NATSEnvelope
from eirvah_contracts.signals import NormalizedSignalEnvelope, RawSignalEnvelope, SignalValueType
from eirvah_observability.health import HealthApp
from eirvah_observability.logging import configure_logging
from eirvah_observability.metrics import make_counter
from nats.aio.msg import Msg
from pydantic import BaseModel

from data_converter.config import DataConverterSettings

_log = structlog.get_logger("data-converter")

SUBJECT = "uns.work.convert"


class ConversionRule(BaseModel):
    node_id: str
    value_type: SignalValueType
    unit: str
    drop_bad_quality: bool = False
    scale: float | None = None
    offset: float | None = None


def load_rules(path: Path) -> dict[str, ConversionRule]:
    raw = yaml.safe_load(path.read_text())
    return {r["node_id"]: ConversionRule.model_validate(r) for r in raw["rules"]}


def apply_conversion(
    raw: RawSignalEnvelope,
    rule: ConversionRule,
) -> NormalizedSignalEnvelope | None:
    if rule.drop_bad_quality and raw.quality == "bad":
        return None
    value: Any = raw.value
    if rule.scale is not None:
        value = float(value) * rule.scale
    if rule.offset is not None:
        value = float(value) + rule.offset
    return NormalizedSignalEnvelope(
        node_id=raw.node_id,
        value=value,
        value_type=rule.value_type,
        unit=rule.unit,
        quality=raw.quality,
        source_timestamp=raw.source_timestamp,
        received_at=raw.received_at,
    )


def handle_convert_request(
    envelope: NATSEnvelope,
    rules: dict[str, ConversionRule],
) -> NATSEnvelope:
    try:
        raw = RawSignalEnvelope.model_validate(envelope.payload)
        rule = rules.get(raw.node_id)
        if rule is None:
            return NATSEnvelope(
                correlation_id=envelope.correlation_id,
                status="error",
                error=EnvelopeError(
                    kind="UnknownNode",
                    message=f"no conversion rule for node_id {raw.node_id!r}",
                ),
            )
        normalized = apply_conversion(raw, rule)
        if normalized is None:
            return NATSEnvelope(
                correlation_id=envelope.correlation_id,
                status="error",
                error=EnvelopeError(
                    kind="DroppedQuality",
                    message=f"dropped bad-quality reading for {raw.node_id!r}",
                ),
            )
        return NATSEnvelope(
            correlation_id=envelope.correlation_id,
            payload=normalized.model_dump(mode="json"),
        )
    except Exception as exc:
        return NATSEnvelope(
            correlation_id=envelope.correlation_id,
            status="error",
            error=EnvelopeError(kind=type(exc).__name__, message=str(exc)[:200]),
        )


class DataConverterWorker:
    def __init__(self, settings: DataConverterSettings) -> None:
        self._settings = settings
        self._rules: dict[str, ConversionRule] = {}
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
        self._rules = load_rules(self._settings.rules_path)
        self._bus = BusClient(servers=self._settings.nats_servers, name="data-converter")
        await self._bus.connect()
        await subscribe_queue_group(nc=self._bus.nc, subject=SUBJECT, handler=self._handle)
        self._ready = True
        _log.info("data_converter_ready", subject=SUBJECT, rules=len(self._rules))
        await asyncio.get_event_loop().create_future()

    async def _handle(self, msg: Msg) -> None:
        try:
            envelope = NATSEnvelope.model_validate_json(msg.data)
        except Exception as exc:
            _log.warning("invalid_envelope", error=str(exc))
            return
        reply = handle_convert_request(envelope, self._rules)
        self._handled.labels(worker="data-converter", outcome=reply.status).inc()
        await msg.respond(reply.model_dump_json().encode())


async def run(settings: DataConverterSettings) -> None:
    configure_logging(level=settings.log_level)
    worker = DataConverterWorker(settings)
    health = HealthApp(is_ready=worker.is_ready)
    http_cfg = uvicorn.Config(
        health.asgi,
        host="0.0.0.0",
        port=settings.http_port,
        log_level=settings.log_level.lower(),
    )
    http = uvicorn.Server(http_cfg)
    await asyncio.gather(worker.run(), http.serve())
