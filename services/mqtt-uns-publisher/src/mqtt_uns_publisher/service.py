"""MQTT UNS publisher NATS req/rep worker (spec §3.1)."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import aiomqtt
import structlog
import uvicorn
from eirvah_bus.client import BusClient
from eirvah_bus.consumer import subscribe_queue_group
from eirvah_contracts.envelope import EnvelopeError, NATSEnvelope
from eirvah_contracts.pipeline import PublishRequest
from eirvah_contracts.telemetry import TelemetryPayload, TelemetrySource, TelemetryTimestamps
from eirvah_observability.health import HealthApp
from eirvah_observability.logging import configure_logging
from eirvah_observability.metrics import make_counter, make_gauge
from nats.aio.msg import Msg

from mqtt_uns_publisher.config import MqttPublisherSettings

_log = structlog.get_logger("mqtt-uns-publisher")
SUBJECT = "uns.work.publish"


def build_telemetry_payload(req: PublishRequest) -> TelemetryPayload:
    return TelemetryPayload(
        correlation_id=req.correlation_id,
        value=req.value,
        value_type=req.value_type,
        semantic_type=req.semantic_type,
        unit=req.unit,
        quality=req.quality,
        uns_path=req.uns_path,
        source=TelemetrySource(
            protocol="opcua",
            endpoint=req.source_endpoint,
            node_id=req.source_node_id,
        ),
        timestamps=TelemetryTimestamps(
            source=req.source_timestamp,
            edge_ingress=req.edge_ingress,
            edge_publish=datetime.now(UTC),
        ),
    )


class MqttPublisherWorker:
    def __init__(self, settings: MqttPublisherSettings) -> None:
        self._settings = settings
        self._mqtt_client: aiomqtt.Client | None = None
        self._nats_ready = False
        self._mqtt_ready = False
        self._reconnect_event = asyncio.Event()
        self._handled = make_counter(
            "worker_handler_total",
            "Worker handler invocations",
            labelnames=["worker", "outcome"],
        )
        self._conn_state = make_gauge(
            "ingress_connection_state",
            "Connection state",
            labelnames=["ingress", "state"],
        )

    def is_ready(self) -> bool:
        return self._nats_ready and self._mqtt_ready

    async def run(self) -> None:
        bus = BusClient(servers=self._settings.nats_servers, name="mqtt-uns-publisher")
        await bus.connect()
        await subscribe_queue_group(nc=bus.nc, subject=SUBJECT, handler=self._handle)
        self._nats_ready = True

        while True:
            try:
                async with aiomqtt.Client(
                    hostname=self._settings.mqtt_host,
                    port=self._settings.mqtt_port,
                    username=self._settings.mqtt_username,
                    password=self._settings.mqtt_password,
                    identifier=self._settings.mqtt_client_id,
                ) as client:
                    self._mqtt_client = client
                    self._mqtt_ready = True
                    self._reconnect_event.clear()
                    self._conn_state.labels(ingress="mqtt", state="connected").set(1)
                    self._conn_state.labels(ingress="mqtt", state="disconnected").set(0)
                    _log.info("mqtt_connected", host=self._settings.mqtt_host)
                    await self._reconnect_event.wait()
            except Exception as exc:
                self._mqtt_ready = False
                self._mqtt_client = None
                self._conn_state.labels(ingress="mqtt", state="connected").set(0)
                self._conn_state.labels(ingress="mqtt", state="disconnected").set(1)
                _log.warning("mqtt_disconnected", error=str(exc))
                await asyncio.sleep(self._settings.reconnect_delay_s)

    async def _handle(self, msg: Msg) -> None:
        correlation_id = "UNKNOWN"
        try:
            envelope = NATSEnvelope.model_validate_json(msg.data)
            correlation_id = envelope.correlation_id
            req = PublishRequest.model_validate(envelope.payload)

            if self._mqtt_client is None:
                raise RuntimeError("MQTT not connected")

            telemetry = build_telemetry_payload(req)
            try:
                await self._mqtt_client.publish(
                    req.uns_topic,
                    payload=telemetry.model_dump_json().encode(),
                    qos=self._settings.qos,
                    retain=self._settings.retain,
                )
            except aiomqtt.MqttError as mqtt_exc:
                self._reconnect_event.set()
                raise mqtt_exc

            self._handled.labels(worker="mqtt-uns-publisher", outcome="ok").inc()
            reply = NATSEnvelope(correlation_id=correlation_id)
        except Exception as exc:
            self._handled.labels(worker="mqtt-uns-publisher", outcome="error").inc()
            reply = NATSEnvelope(
                correlation_id=correlation_id,
                status="error",
                error=EnvelopeError(kind=type(exc).__name__, message=str(exc)[:200]),
            )
        await msg.respond(reply.model_dump_json().encode())


async def run(settings: MqttPublisherSettings) -> None:
    configure_logging(level=settings.log_level)
    worker = MqttPublisherWorker(settings)
    health = HealthApp(is_ready=worker.is_ready)
    http_cfg = uvicorn.Config(
        health.asgi,
        host="0.0.0.0",
        port=settings.http_port,
        log_level=settings.log_level.lower(),
    )
    http = uvicorn.Server(http_cfg)
    await asyncio.gather(worker.run(), http.serve())
