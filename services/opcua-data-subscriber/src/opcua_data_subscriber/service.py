"""OPC UA data subscriber service (spec §3.1)."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import structlog
import uvicorn
import yaml
from asyncua import Client
from eirvah_bus.client import BusClient
from eirvah_bus.request_reply import BUS_HEADER_CORRELATION_ID
from eirvah_contracts.envelope import NATSEnvelope
from eirvah_contracts.signals import Quality, RawSignalEnvelope, SignalValueType
from eirvah_contracts.ulid import generate_correlation_id
from eirvah_observability.health import HealthApp
from eirvah_observability.logging import configure_logging
from eirvah_observability.metrics import make_gauge
from pydantic import BaseModel

from opcua_data_subscriber.config import SubscriberSettings

_log = structlog.get_logger("opcua-data-subscriber")


# ---------------------------------------------------------------------------
# Config models (loaded from opcua-node-list.yaml)
# ---------------------------------------------------------------------------

class NodeConfig(BaseModel):
    browse_names: list[str]
    alias: str


class NodeListConfig(BaseModel):
    endpoint: str
    namespace_uri: str
    publishing_interval_ms: int = 500
    nodes: list[NodeConfig]


def load_node_list(path: Path) -> NodeListConfig:
    raw = yaml.safe_load(path.read_text())
    return NodeListConfig.model_validate(raw)


# ---------------------------------------------------------------------------
# Pure helper functions (unit-testable without live connections)
# ---------------------------------------------------------------------------

def detect_value_type(value: Any) -> SignalValueType:
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int64"
    if isinstance(value, float):
        return "double"
    return "string"


def build_raw_envelope(
    *,
    alias: str,
    value: Any,
    source_endpoint: str,
    data_value: Any,
) -> RawSignalEnvelope:
    sc = data_value.StatusCode
    if sc.is_bad():
        quality: Quality = "bad"
    elif sc.is_uncertain():
        quality = "uncertain"
    else:
        quality = "good"

    src_ts = data_value.SourceTimestamp
    srv_ts = data_value.ServerTimestamp
    if src_ts is None or not hasattr(src_ts, "tzinfo"):
        src_ts = datetime.now(UTC)
    elif src_ts.tzinfo is None:
        src_ts = src_ts.replace(tzinfo=UTC)
    if srv_ts is None or not hasattr(srv_ts, "tzinfo"):
        srv_ts = datetime.now(UTC)
    elif srv_ts.tzinfo is None:
        srv_ts = srv_ts.replace(tzinfo=UTC)

    return RawSignalEnvelope(
        source_endpoint=source_endpoint,
        node_id=alias,
        value=value,
        value_type=detect_value_type(value),
        quality=quality,
        source_timestamp=src_ts,
        server_timestamp=srv_ts,
        received_at=datetime.now(UTC),
    )


def wrap_in_nats_envelope(raw: RawSignalEnvelope) -> NATSEnvelope:
    return NATSEnvelope(
        correlation_id=generate_correlation_id(),
        payload=raw.model_dump(mode="json"),
    )


# ---------------------------------------------------------------------------
# OPC UA subscription handler
# ---------------------------------------------------------------------------

class _DataChangeHandler:  # asyncua calls datachange_notification by duck-typing
    def __init__(
        self,
        alias_map: dict[str, str],
        endpoint: str,
        on_message: Any,
    ) -> None:
        self._alias_map = alias_map
        self._endpoint = endpoint
        self._on_message = on_message

    async def datachange_notification(self, node: Any, val: Any, data: Any) -> None:
        node_key = str(node.nodeid)
        alias = self._alias_map.get(node_key)
        if alias is None:
            return
        try:
            raw = build_raw_envelope(
                alias=alias,
                value=val,
                source_endpoint=self._endpoint,
                data_value=data,
            )
            envelope = wrap_in_nats_envelope(raw)
            await self._on_message(envelope)
        except Exception as exc:
            _log.warning("datachange_handler_error", error=str(exc))


# ---------------------------------------------------------------------------
# Runtime
# ---------------------------------------------------------------------------

class SubscriberRuntime:
    def __init__(self, settings: SubscriberSettings) -> None:
        self._settings = settings
        self._bus: BusClient | None = None
        self._ready = False
        self._connection_state = make_gauge(
            "ingress_connection_state",
            "1 when the ingress connection is up, 0 otherwise",
            labelnames=["ingress", "state"],
        )

    def is_ready(self) -> bool:
        return self._ready

    async def run(self) -> None:
        cfg = load_node_list(self._settings.node_list_path)
        self._bus = BusClient(servers=self._settings.nats_servers, name="opcua-data-subscriber")
        await self._bus.connect()
        _log.info("nats_connected", servers=self._settings.nats_servers)

        while True:
            try:
                await self._subscribe_loop(cfg)
            except Exception as exc:
                self._ready = False
                self._connection_state.labels(ingress="opcua", state="disconnected").set(1)
                self._connection_state.labels(ingress="opcua", state="connected").set(0)
                _log.warning("opcua_disconnected", error=str(exc))
                await asyncio.sleep(self._settings.reconnect_delay_s)

    async def _subscribe_loop(self, cfg: NodeListConfig) -> None:
        async with Client(url=cfg.endpoint) as client:
            ns_idx = await client.get_namespace_index(cfg.namespace_uri)
            objects = client.nodes.objects

            alias_map: dict[str, str] = {}
            nodes_to_subscribe = []
            for node_cfg in cfg.nodes:
                path = [f"{ns_idx}:{name}" for name in node_cfg.browse_names]
                node = await objects.get_child(path)
                alias_map[str(node.nodeid)] = node_cfg.alias
                nodes_to_subscribe.append(node)

            handler = _DataChangeHandler(
                alias_map=alias_map,
                endpoint=cfg.endpoint,
                on_message=self._publish,
            )
            sub = await client.create_subscription(cfg.publishing_interval_ms, handler)
            await sub.subscribe_data_change(nodes_to_subscribe)

            self._ready = True
            self._connection_state.labels(ingress="opcua", state="connected").set(1)
            self._connection_state.labels(ingress="opcua", state="disconnected").set(0)
            _log.info(
                "opcua_subscribed",
                node_count=len(nodes_to_subscribe),
                endpoint=cfg.endpoint,
            )

            await asyncio.get_event_loop().create_future()  # run until exception

    async def _publish(self, envelope: NATSEnvelope) -> None:
        assert self._bus is not None
        headers = {BUS_HEADER_CORRELATION_ID: envelope.correlation_id}
        await self._bus.nc.publish(
            "uns.ingress.raw",
            envelope.model_dump_json().encode(),
            headers=headers,
        )


async def run(settings: SubscriberSettings) -> None:
    configure_logging(level=settings.log_level)
    runtime = SubscriberRuntime(settings)
    health = HealthApp(is_ready=runtime.is_ready)

    http_cfg = uvicorn.Config(
        health.asgi,
        host="0.0.0.0",
        port=settings.http_port,
        log_level=settings.log_level.lower(),
    )
    http = uvicorn.Server(http_cfg)
    await asyncio.gather(runtime.run(), http.serve())
