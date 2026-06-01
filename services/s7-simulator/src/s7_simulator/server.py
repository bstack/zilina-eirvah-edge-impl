"""S7 TCP server — Capper (DB1) + Palletizer (DB2) data blocks."""
from __future__ import annotations

import asyncio
import random
import struct
import threading
from dataclasses import dataclass, field

import structlog
import uvicorn
from eirvah_observability.health import HealthApp
from eirvah_observability.logging import configure_logging
from snap7 import server as snap7_server
from snap7.type import SrvArea

from s7_simulator.config import SimulatorSettings
from s7_simulator.metrics import SimulatorMetrics

_log = structlog.get_logger("s7-simulator")

DB1_SIZE = 8   # Capper: REAL(4) + INT(2) + INT(2)
DB2_SIZE = 8   # Palletizer: INT(2) + INT(2) + INT(2) + padding(2)


def encode_real(buf: bytearray, offset: int, value: float) -> None:
    struct.pack_into(">f", buf, offset, value)


def encode_int(buf: bytearray, offset: int, value: int) -> None:
    struct.pack_into(">h", buf, offset, value)


@dataclass
class CapperBlock:
    torque_nm: float = 2.5
    cap_presence: int = 1
    rejects_per_min: int = 0

    def tick(self, *, rng: random.Random) -> None:
        self.torque_nm = max(1.5, min(4.0, self.torque_nm + rng.uniform(-0.2, 0.2)))
        self.cap_presence = 0 if rng.random() < 0.002 else 1
        if rng.random() < 0.01:
            self.rejects_per_min = min(self.rejects_per_min + 1, 10)
        elif self.rejects_per_min > 0 and rng.random() < 0.1:
            self.rejects_per_min -= 1

    def to_bytes(self) -> bytearray:
        buf = bytearray(DB1_SIZE)
        encode_real(buf, 0, self.torque_nm)
        encode_int(buf, 4, self.cap_presence)
        encode_int(buf, 6, self.rejects_per_min)
        return buf


@dataclass
class PalletizerBlock:
    layer_count: int = 0
    pallet_complete: int = 0
    cycles_per_hr: int = 12
    _ticks_since_layer: int = field(default=11, repr=False)

    def tick(self, *, rng: random.Random) -> None:
        self.pallet_complete = 0
        self._ticks_since_layer += 1
        if self._ticks_since_layer >= 12:
            self._ticks_since_layer = 0
            self.layer_count += 1
            if self.layer_count >= 10:
                self.layer_count = 0
                self.pallet_complete = 1
        self.cycles_per_hr = max(8, min(16, self.cycles_per_hr + rng.randint(-1, 1)))

    def to_bytes(self) -> bytearray:
        buf = bytearray(DB2_SIZE)
        encode_int(buf, 0, self.layer_count)
        encode_int(buf, 2, self.pallet_complete)
        encode_int(buf, 4, self.cycles_per_hr)
        return buf


class SimulatorRuntime:
    def __init__(self, settings: SimulatorSettings) -> None:
        self._settings = settings
        self._rng = random.Random(settings.seed)
        self._metrics = SimulatorMetrics()
        self._capper = CapperBlock()
        self._palletizer = PalletizerBlock()
        self._lock = threading.Lock()
        self._server: snap7_server.Server | None = None
        self._db1 = bytearray(DB1_SIZE)
        self._db2 = bytearray(DB2_SIZE)
        self._ready = False

    def is_ready(self) -> bool:
        return self._ready

    def _sync_buffers(self) -> None:
        with self._lock:
            db1_bytes = self._capper.to_bytes()
            db2_bytes = self._palletizer.to_bytes()
        self._db1[:] = db1_bytes
        self._db2[:] = db2_bytes

    async def _tick_loop(self) -> None:
        interval = self._settings.tick_rate_ms / 1000.0
        while True:
            await asyncio.sleep(interval)
            with self._lock:
                self._capper.tick(rng=self._rng)
                self._palletizer.tick(rng=self._rng)
            self._sync_buffers()
            self._metrics.set_capper(
                self._capper.torque_nm,
                self._capper.cap_presence,
                self._capper.rejects_per_min,
            )
            self._metrics.set_palletizer(
                self._palletizer.layer_count,
                self._palletizer.pallet_complete,
                self._palletizer.cycles_per_hr,
            )

    async def run(self) -> None:
        self._server = snap7_server.Server()
        self._server.register_area(SrvArea.DB, 1, self._db1)
        self._server.register_area(SrvArea.DB, 2, self._db2)
        self._sync_buffers()
        self._server.start(tcp_port=102)
        self._ready = True
        _log.info("s7_simulator_starting", host=self._settings.host)
        await self._tick_loop()


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
