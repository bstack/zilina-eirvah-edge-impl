"""Modbus TCP data subscriber — polls registers, publishes to uns.ingress.raw."""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path

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
from pymodbus.client import AsyncModbusTcpClient

from modbus_data_subscriber.config import SubscriberSettings

_log = structlog.get_logger("modbus-data-subscriber")
_NATS_SUBJECT = "uns.ingress.raw"


class RegisterConfig(BaseModel):
    address: int
    alias: str
    scale: float = 1.0
    value_type: SignalValueType = "double"


class RegisterMapConfig(BaseModel):
    host: str
    port: int = 502
    unit_id: int = 1
    poll_interval_ms: int = 500
    registers: list[RegisterConfig]


def load_register_map(path: Path) -> RegisterMapConfig:
    raw = yaml.safe_load(path.read_text())
    return RegisterMapConfig.model_validate(raw)


def apply_scale(
    raw: int,
    *,
    scale: float,
    value_type: SignalValueType,
) -> float | int:
    if value_type == "int64":
        return int(raw)
    return raw / scale


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
            "modbus_ingress_connection_state",
            "1 when the Modbus TCP connection is up, 0 otherwise",
            labelnames=["ingress", "state"],
        )

    def is_ready(self) -> bool:
        return self._ready

    async def run(self) -> None:
        cfg = load_register_map(self._settings.register_map_path)
        self._bus = BusClient(servers=self._settings.nats_servers, name="modbus-data-subscriber")
        await self._bus.connect()
        _log.info("nats_connected", servers=self._settings.nats_servers)

        while True:
            try:
                await self._poll_loop(cfg)
            except Exception as exc:
                self._ready = False
                self._connection_state.labels(ingress="modbus", state="connected").set(0)
                self._connection_state.labels(ingress="modbus", state="disconnected").set(1)
                _log.warning("modbus_disconnected", error=str(exc))
                await asyncio.sleep(self._settings.reconnect_delay_s)

    async def _poll_loop(self, cfg: RegisterMapConfig) -> None:
        source_endpoint = f"modbus-tcp://{cfg.host}:{cfg.port}"
        interval = cfg.poll_interval_ms / 1000.0
        addresses = [r.address for r in cfg.registers]
        start = min(addresses)
        count = max(addresses) - start + 1

        async with AsyncModbusTcpClient(cfg.host, port=cfg.port) as client:
            self._ready = True
            self._connection_state.labels(ingress="modbus", state="connected").set(1)
            self._connection_state.labels(ingress="modbus", state="disconnected").set(0)
            _log.info("modbus_connected", host=cfg.host, port=cfg.port)

            while True:
                await asyncio.sleep(interval)
                result = await client.read_holding_registers(
                    address=start, count=count, slave=cfg.unit_id
                )
                if result.isError():
                    raise ConnectionError(f"Modbus read failed: {result}")

                received_at = datetime.now(UTC)
                for reg in cfg.registers:
                    raw = result.registers[reg.address - start]
                    value = apply_scale(raw, scale=reg.scale, value_type=reg.value_type)
                    envelope = wrap_in_nats_envelope(
                        build_raw_envelope(
                            alias=reg.alias,
                            value=value,
                            value_type=reg.value_type,
                            source_endpoint=source_endpoint,
                            received_at=received_at,
                        )
                    )
                    await self._publish(envelope)

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
