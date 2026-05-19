"""AMQP actuation event subscriber — bridges RabbitMQ to NATS (spec §3.2)."""

from __future__ import annotations

import asyncio

import aio_pika
import structlog
import uvicorn
from eirvah_bus.client import BusClient
from eirvah_contracts.actuation import ActuationRequest
from eirvah_contracts.envelope import NATSEnvelope
from eirvah_observability.health import HealthApp
from eirvah_observability.logging import configure_logging
from eirvah_observability.metrics import make_counter

from amqp_actuation_event_subscriber.config import AmqpSubscriberSettings

_log = structlog.get_logger("amqp-actuation-event-subscriber")
NATS_SUBJECT = "act.ingress.requested"


def build_nats_envelope(amqp_body: bytes) -> NATSEnvelope:
    req = ActuationRequest.model_validate_json(amqp_body)
    return NATSEnvelope(
        correlation_id=req.correlation_id,
        payload=req.model_dump(mode="json"),
    )


class AmqpSubscriberRuntime:
    def __init__(self, settings: AmqpSubscriberSettings) -> None:
        self._settings = settings
        self._ready = False
        self._handled = make_counter(
            "amqp_subscriber_messages_total",
            "AMQP messages processed",
            labelnames=["outcome"],
        )

    def is_ready(self) -> bool:
        return self._ready

    async def run(self) -> None:
        bus = BusClient(servers=self._settings.nats_servers, name="amqp-actuation-event-subscriber")
        await bus.connect()
        _log.info("nats_connected")

        connection = await aio_pika.connect_robust(self._settings.amqp_url)
        async with connection:
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=self._settings.amqp_prefetch)
            queue = await channel.declare_queue(self._settings.amqp_queue, durable=True)
            self._ready = True
            _log.info("amqp_subscriber_ready", queue=self._settings.amqp_queue)

            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    try:
                        envelope = build_nats_envelope(message.body)
                        await bus.nc.publish(
                            NATS_SUBJECT,
                            envelope.model_dump_json().encode(),
                        )
                        await message.ack()
                        self._handled.labels(outcome="ok").inc()
                        _log.debug(
                            "forwarded_to_nats",
                            correlation_id=envelope.correlation_id,
                        )
                    except Exception as exc:
                        await message.nack(requeue=True)
                        self._handled.labels(outcome="error").inc()
                        _log.warning("forward_failed", error=str(exc))


async def run(settings: AmqpSubscriberSettings) -> None:
    configure_logging(level=settings.log_level)
    runtime = AmqpSubscriberRuntime(settings)
    health = HealthApp(is_ready=runtime.is_ready)
    http_cfg = uvicorn.Config(
        health.asgi,
        host="0.0.0.0",
        port=settings.http_port,
        log_level=settings.log_level.lower(),
    )
    http = uvicorn.Server(http_cfg)
    await asyncio.gather(runtime.run(), http.serve())
