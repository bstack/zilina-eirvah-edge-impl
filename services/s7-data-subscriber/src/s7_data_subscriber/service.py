"""S7 TCP data subscriber — polls DB1+DB2, publishes to uns.ingress.raw."""
from __future__ import annotations

import asyncio
import struct
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import structlog
import uvicorn
import yaml
from eirvah_bus.client import BusClient
from eirvah_bus.request_reply import BUS_HEADER_CORRELATION_ID
from eirvah_contracts.envelope import NATSEnvelope
from eirvah_contracts.signals import RawSignalEnvelope, SignalValueType
from eirvah_contracts.ulid import generate_correlation_id
from eirvah_observability.health import HealthApp
from eirvah_observability.logging import configure_logging
from eirvah_observability.metrics import make_gauge
from pydantic import BaseModel
from snap7.client import Client as S7Client

from s7_data_subscriber.config import SubscriberSettings

_log = structlog.get_logger("s7-data-subscriber")
_NATS_SUBJECT = "uns.ingress.raw"
S7SignalType = Literal["real", "int"]


class SignalConfig(BaseModel):
    offset: int
    type: S7SignalType
    alias: str
    value_type: SignalValueType


class BlockConfig(BaseModel):
    db: int
    size: int
    signals: list[SignalConfig]


class S7UnitMapConfig(BaseModel):
    host: str
    port: int = 102
    rack: int = 0
    slot: int = 1
    poll_interval_ms: int = 1000
    data_blocks: list[BlockConfig]


def load_s7_unit_map(path: Path) -> S7UnitMapConfig:
    return S7UnitMapConfig.model_validate(yaml.safe_load(path.read_text()))


def decode_real(buf: bytes | bytearray, offset: int) -> float:
    return float(struct.unpack_from(">f", buf, offset)[0])


def decode_int(buf: bytes | bytearray, offset: int) -> int:
    return int(struct.unpack_from(">h", buf, offset)[0])


def build_raw_envelope(
    *,
    alias: str,
    value: float | int,
    value_type: SignalValueType,
    source_endpoint: str,
    received_at: datetime,
) -> RawSignalEnvelope:
    return RawSignalEnvelope(
        source_endpoint=source_endpoint,
        node_id=alias,
        value=value,
        value_type=value_type,
        quality="good",
        source_timestamp=received_at,
        server_timestamp=received_at,
        received_at=received_at,
    )


def wrap_in_nats_envelope(raw: RawSignalEnvelope) -> NATSEnvelope:
    return NATSEnvelope(
        correlation_id=generate_correlation_id(),
        payload=raw.model_dump(mode="json"),
    )


class SubscriberRuntime:
    def __init__(self, settings: SubscriberSettings) -> None:
        self._settings = settings
        self._bus: BusClient | None = None
        self._ready = False
        self._connection_state = make_gauge(
            "s7_ingress_connection_state",
            "1 when S7 TCP connection is up, 0 otherwise",
            labelnames=["ingress", "state"],
        )

    def is_ready(self) -> bool:
        return self._ready

    async def run(self) -> None:
        cfg = load_s7_unit_map(self._settings.unit_map_path)
        self._bus = BusClient(servers=self._settings.nats_servers, name="s7-data-subscriber")
        await self._bus.connect()
        _log.info("nats_connected")
        while True:
            try:
                await self._poll_loop(cfg)
            except Exception as exc:
                self._ready = False
                self._connection_state.labels(ingress="s7", state="connected").set(0)
                self._connection_state.labels(ingress="s7", state="disconnected").set(1)
                _log.warning("s7_disconnected", error=str(exc))
                await asyncio.sleep(self._settings.reconnect_delay_s)

    async def _poll_loop(self, cfg: S7UnitMapConfig) -> None:
        source_endpoint = f"s7://{cfg.host}:{cfg.port}"
        interval = cfg.poll_interval_ms / 1000.0
        client = S7Client()
        await asyncio.to_thread(client.connect, cfg.host, cfg.rack, cfg.slot, cfg.port)
        self._ready = True
        self._connection_state.labels(ingress="s7", state="connected").set(1)
        self._connection_state.labels(ingress="s7", state="disconnected").set(0)
        _log.info("s7_connected", host=cfg.host, port=cfg.port)
        try:
            while True:
                await asyncio.sleep(interval)
                received_at = datetime.now(UTC)
                for block in cfg.data_blocks:
                    data = await asyncio.to_thread(client.db_read, block.db, 0, block.size)
                    for sig in block.signals:
                        value: float | int
                        if sig.type == "real":
                            value = decode_real(data, sig.offset)
                        else:
                            value = decode_int(data, sig.offset)
                        env = wrap_in_nats_envelope(
                            build_raw_envelope(
                                alias=sig.alias,
                                value=value,
                                value_type=sig.value_type,
                                source_endpoint=source_endpoint,
                                received_at=received_at,
                            )
                        )
                        await self._publish(env)
        finally:
            await asyncio.to_thread(client.disconnect)

    async def _publish(self, envelope: NATSEnvelope) -> None:
        assert self._bus is not None
        headers = {BUS_HEADER_CORRELATION_ID: envelope.correlation_id}
        await self._bus.nc.publish(
            _NATS_SUBJECT,
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
