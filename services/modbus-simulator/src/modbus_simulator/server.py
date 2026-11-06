"""Modbus TCP server + tick loop."""

from __future__ import annotations

import asyncio
import random
import secrets
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

import structlog
import uvicorn
from eirvah_contracts.inspection import BottleInspection, decode_inspection, encode_inspection
from eirvah_controls import MotorControl, RejectControl, ValveControl
from eirvah_controls.inspection import InspectionStream
from eirvah_controls.recovery import SourceJournal
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
    # Filler (addresses 0-2)
    fill_level_raw: int = 750  # 75.0% (scale x10)
    motor_state: int = 1  # running
    throughput_raw: int = 80  # 0.80 bottles/s (scale x100)
    # Conveyor (addresses 3-5)
    belt_speed_raw: int = 50  # 0.50 m/s (scale x100)
    jam_detected: int = 0
    bottle_count: int = 0  # synthetic IDs cycle through 1-65535
    # Reject Station (addresses 6-7)
    reject_count: int = 0  # cumulative accepted bottle-rejection commands
    conveyor_active: int = 1

    conveyor: MotorControl = field(init=False)
    valve: ValveControl = field(default_factory=ValveControl)
    reject: RejectControl = field(default_factory=RejectControl)

    def __post_init__(self) -> None:
        self.conveyor = MotorControl(max_speed=80, setpoint=self.belt_speed_raw)
        self.reject.count = self.reject_count

    def as_list(self) -> list[int]:
        return [
            self.fill_level_raw,
            self.motor_state,
            self.throughput_raw,
            self.belt_speed_raw,
            self.jam_detected,
            self.bottle_count,
            self.reject_count,
            self.conveyor_active,
        ]

    def tick(
        self, *, rng: random.Random, delta_max: int = 20, jam_probability: float = 0.001
    ) -> None:
        span = abs(delta_max)
        if self.valve.opened:
            self.fill_level_raw = max(500, min(950, self.fill_level_raw + rng.randint(-span, span)))
        self.throughput_raw = 80 if self.valve.opened else 0
        if rng.random() < jam_probability:
            self.conveyor.apply(4, 1)
        self.belt_speed_raw = self.conveyor.speed
        self.jam_detected = int(self.conveyor.faulted)
        self.conveyor_active = int(self.belt_speed_raw > 0)
        if self.belt_speed_raw:
            self.bottle_count = self.bottle_count % 65535 + 1
            self.reject.observe(self.bottle_count)
        self.reject_count = self.reject.count


class SimulatorRuntime:
    def __init__(self, settings: SimulatorSettings) -> None:
        self._settings = settings
        self._rng = random.Random(settings.seed)
        self._metrics = SimulatorMetrics()
        self._block = RegisterBlock()
        self._inspection = InspectionStream(
            session=secrets.randbits(64) or 1,
            every_n_bottles=settings.inspection_every_n_bottles,
            fail_every_n_inspections=settings.inspection_fail_every_n,
        )
        self._context: ModbusServerContext | None = None
        self._ready = False
        self._journal: SourceJournal | None = None
        self._acks: dict[str, list[int]] = {}
        self._boot = 0

    def _snapshot(self) -> dict[str, Any]:
        block = asdict(self._block)
        block["reject"]["_present"] = list(self._block.reject._present)
        block["reject"]["_rejected"] = sorted(self._block.reject._rejected)
        return dict(
            block=block,
            inspection=asdict(self._inspection),
            acks=self._acks,
            rng=self._rng.getstate(),
        )

    def _open_recovery(self) -> None:
        if self._settings.journal_path is None:
            return
        self._journal = SourceJournal(self._settings.journal_path, self._settings.outbox_capacity)
        self._boot = self._journal.boot()
        old = self._journal.state()
        if old:
            block = old["block"]
            for key, value in block.items():
                if key not in ("conveyor", "valve", "reject"):
                    setattr(self._block, key, value)
            self._block.conveyor = MotorControl(**block["conveyor"])
            self._block.valve = ValveControl(**block["valve"])
            reject = block["reject"]
            reject["_present"] = deque(reject["_present"])
            reject["_rejected"] = set(reject["_rejected"])
            self._block.reject = RejectControl(**reject)
            for key, value in old["inspection"].items():
                setattr(self._inspection, key, value)
            self._inspection.session = self._inspection.session % (2**64 - 1) + 1
            self._inspection._sequence = 0
            self._acks = old["acks"]
            rng = old["rng"]
            self._rng.setstate((rng[0], tuple(rng[1]), rng[2]))
        self._journal.commit(self._snapshot())

    def is_ready(self) -> bool:
        return self._ready

    def _build_context(self) -> ModbusServerContext:
        store = ModbusSlaveContext(  # type: ignore[no-untyped-call]
            hr=ModbusSequentialDataBlock(0, [0] * 80),  # type: ignore[no-untyped-call]
        )
        context = ModbusServerContext(  # type: ignore[no-untyped-call]
            slaves={self._settings.unit_id: store}, single=False
        )
        context[self._settings.unit_id].setValues(_HR, 0, self._block.as_list())
        for address, ack in ((20, 50), (24, 52), (28, 54)):
            if str(address) in self._acks:
                context[self._settings.unit_id].setValues(_HR, ack, self._acks[str(address)])
        return context

    def _process_controls(self) -> None:
        assert self._context is not None
        store = self._context[self._settings.unit_id]
        if self._journal is not None:
            values = store.getValues(_HR, 70, 5)
            session = sum(values[i] << (48 - i * 16) for i in range(4))
            if session and values[4]:
                self._journal.ack(f"{session:016x}:{values[4]:04x}")
        for address, ack, control in (
            (20, 50, self._block.conveyor),
            (24, 52, self._block.valve),
            (28, 54, self._block.reject),
        ):
            sequence, operation, value = store.getValues(_HR, address, 3)
            previous = store.getValues(_HR, ack, 2)[0]
            if sequence and sequence != previous:
                before = self._block.reject.count
                status = control.apply(operation, value)
                self._acks[str(address)] = [sequence, status]
                self._block.reject_count = self._block.reject.count
                if self._journal is not None:
                    effect = dict(
                        target=address,
                        sequence=sequence,
                        operation=operation,
                        value=value,
                        status=status,
                        before_count=before,
                        after_count=self._block.reject.count,
                        device_boot=self._boot,
                    )
                    self._journal.commit(self._snapshot(), effect=effect)
                store.setValues(_HR, ack, [sequence, status])
                _log.info("device_command", target=address, operation=operation, outcome=status)

    async def _tick_loop(self) -> None:
        interval = self._settings.tick_rate_ms / 1000.0
        while True:
            await asyncio.sleep(interval)
            self._process_controls()
            previous_bottle = self._block.bottle_count
            previous_inspected = self._inspection.inspected
            full = (
                self._journal is not None
                and self._journal.pending() >= self._settings.outbox_capacity
            )
            if full:
                self._block.belt_speed_raw = 0
                self._block.throughput_raw = 0
                self._block.conveyor_active = 0
            else:
                self._block.tick(rng=self._rng, jam_probability=self._settings.jam_probability)
                if self._block.bottle_count != previous_bottle:
                    self._inspection.observe(self._block.bottle_count, datetime.now(UTC))
            if self._journal is not None:
                event = (
                    decode_inspection(self._inspection.registers())
                    if self._inspection.inspected != previous_inspected
                    else None
                )
                self._journal.commit(
                    self._snapshot(), event.model_dump(mode="json") if event else None
                )
                self._metrics.set_recovery(self._journal.pending(), int(full))
            if self._context is not None:
                self._context[self._settings.unit_id].setValues(_HR, 0, self._block.as_list())
                self._context[self._settings.unit_id].setValues(
                    _HR,
                    8,
                    [
                        int(self._block.valve.opened),
                        int(self._block.conveyor.running),
                        self._block.conveyor.setpoint,
                        self._block.reject.last_bottle,
                    ],
                )
                self._context[self._settings.unit_id].setValues(
                    _HR, 32, self._inspection.registers()
                )
                if self._journal is not None:
                    head = self._journal.head()
                    event = BottleInspection.model_validate(head) if head else None
                    self._context[self._settings.unit_id].setValues(
                        _HR, 60, encode_inspection(event)
                    )
            self._metrics.set_inspection(
                self._inspection.inspected,
                self._inspection.failed,
                self._inspection.registers()[5],
            )
            self._metrics.set_control_feedback(
                int(self._block.valve.opened),
                int(self._block.conveyor.running),
                self._block.conveyor.setpoint / 100,
                self._block.reject.last_bottle,
            )
            self._metrics.set_fill_level(register_to_scale(self._block.fill_level_raw, scale=10))
            self._metrics.set_motor_state(self._block.motor_state)
            self._metrics.set_throughput(register_to_scale(self._block.throughput_raw, scale=100))
            self._metrics.set_belt_speed(register_to_scale(self._block.belt_speed_raw, scale=100))
            self._metrics.set_jam_detected(self._block.jam_detected)
            self._metrics.set_bottle_count(self._block.bottle_count)
            self._metrics.set_reject_count(self._block.reject_count)
            self._metrics.set_conveyor_active(self._block.conveyor_active)

    async def run(self) -> None:
        self._open_recovery()
        self._context = self._build_context()
        self._ready = True
        _log.info(
            "modbus_simulator_starting",
            host=self._settings.host,
            port=self._settings.port,
            unit_id=self._settings.unit_id,
        )
        await asyncio.gather(
            StartAsyncTcpServer(  # type: ignore[no-untyped-call]
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
