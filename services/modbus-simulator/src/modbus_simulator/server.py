"""Modbus TCP server + tick loop."""
from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal

import structlog
import uvicorn
from eirvah_observability.health import HealthApp
from eirvah_observability.logging import configure_logging
from pymodbus.datastore import (
    ModbusSequentialDataBlock,
    ModbusServerContext,
    ModbusSlaveContext,
)
from pymodbus.server import StartAsyncTcpServer

from modbus_simulator.config import SimulatorSettings
from modbus_simulator.metrics import SimulatorMetrics

_log = structlog.get_logger("modbus-simulator")

_HR = 3  # holding register function code


def scale_to_register(value: float, *, scale: int) -> int:
    return int((Decimal(str(value)) * Decimal(scale)).to_integral_value(rounding=ROUND_HALF_UP))


def register_to_scale(raw: int, *, scale: int) -> float:
    return raw / scale


@dataclass
class RegisterBlock:
    fill_level_raw: int = 750    # 75.0% (scale ×10)
    motor_state: int = 1          # running
    throughput_raw: int = 80      # 0.80 bottles/s

    def as_list(self) -> list[int]:
        return [self.fill_level_raw, self.motor_state, self.throughput_raw]

    def tick(self, *, rng: random.Random, delta_max: int = 20) -> None:
        span = abs(delta_max)
        delta = rng.randint(-span, span)
        self.fill_level_raw = max(500, min(950, self.fill_level_raw + delta))


class SimulatorRuntime:
    def __init__(self, settings: SimulatorSettings) -> None:
        self._settings = settings
        self._rng = random.Random(settings.seed)
        self._metrics = SimulatorMetrics()
        self._block = RegisterBlock()
        self._context: ModbusServerContext | None = None
        self._ready = False

    def is_ready(self) -> bool:
        return self._ready

    def _build_context(self) -> ModbusServerContext:
        store = ModbusSlaveContext(
            hr=ModbusSequentialDataBlock(0, self._block.as_list() + [0] * 7),
        )
        return ModbusServerContext(slaves={self._settings.unit_id: store}, single=False)

    async def _tick_loop(self) -> None:
        interval = self._settings.tick_rate_ms / 1000.0
        while True:
            await asyncio.sleep(interval)
            self._block.tick(rng=self._rng)
            if self._context is not None:
                self._context[self._settings.unit_id].setValues(
                    _HR, 0, self._block.as_list()
                )
            self._metrics.set_fill_level(
                register_to_scale(self._block.fill_level_raw, scale=10)
            )
            self._metrics.set_motor_state(self._block.motor_state)
            self._metrics.set_throughput(
                register_to_scale(self._block.throughput_raw, scale=100)
            )

    async def run(self) -> None:
        self._context = self._build_context()
        self._ready = True
        _log.info(
            "modbus_simulator_starting",
            host=self._settings.host,
            port=self._settings.port,
            unit_id=self._settings.unit_id,
        )
        await asyncio.gather(
            StartAsyncTcpServer(
                context=self._context,
                address=(self._settings.host, self._settings.port),
            ),
            self._tick_loop(),
        )


async def run(settings: SimulatorSettings) -> None:
    configure_logging(level=settings.log_level)
    runtime = SimulatorRuntime(settings)
    health = HealthApp(is_ready=runtime.is_ready)
    http_cfg = uvicorn.Config(
        health.asgi,
        host="0.0.0.0",
        port=settings.http_port,
        log_level=settings.log_level.lower(),
    )
    http = uvicorn.Server(http_cfg)
    await asyncio.gather(runtime.run(), http.serve())
