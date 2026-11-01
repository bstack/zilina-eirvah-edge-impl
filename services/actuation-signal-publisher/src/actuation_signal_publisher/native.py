"""Protocol-native writes with a committed mailbox and matching device acknowledgement."""

from __future__ import annotations

import asyncio
import math
import secrets
import struct
import time
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field
from pymodbus.client import AsyncModbusTcpClient
from snap7.client import Client as S7Client


class NativeTarget(BaseModel):
    protocol: Literal["modbus", "s7"]
    host: str
    port: int = Field(ge=1, le=65535)
    command_address: int = Field(ge=0)
    ack_address: int = Field(ge=0)
    operation: int = Field(ge=1, le=3)
    value_type: Literal["bool", "double", "int64"]
    scale: int = Field(default=1, ge=1)
    minimum: float
    maximum: float
    unit_id: int = 1
    db: int = 3
    rack: int = 0
    slot: int = 1
    timeout: float = Field(default=1.5, gt=0)


def load_native_targets(path: Path) -> dict[str, NativeTarget]:
    entries = yaml.safe_load(path.read_text())["targets"]
    result: dict[str, NativeTarget] = {}
    for entry in entries:
        values = dict(entry)
        topic = values.pop("uns_topic")
        if topic in result:
            raise ValueError(f"duplicate native target: {topic}")
        result[topic] = NativeTarget.model_validate(values)
    return result


def encode_value(target: NativeTarget, value: object) -> int:
    if target.value_type == "bool":
        valid = type(value) is bool
    elif target.value_type == "int64":
        valid = type(value) is int
    else:
        valid = type(value) in (int, float)
    if not valid:
        raise ValueError(f"expected {target.value_type}")
    numeric = float(value)  # type: ignore[arg-type]
    if not math.isfinite(numeric) or not target.minimum <= numeric <= target.maximum:
        raise ValueError("value outside native target range")
    scaled = numeric * target.scale
    raw = round(scaled)
    if not math.isclose(scaled, raw, abs_tol=1e-6) or not 0 <= raw <= 65535:
        raise ValueError("value cannot be represented by target encoding")
    return raw


def _sequence(previous: int) -> int:
    sequence = secrets.randbelow(65535) + 1
    return sequence if sequence != previous else sequence % 65535 + 1


def _check_ack(sequence: int, values: list[int]) -> bool:
    if values[0] != sequence:
        return False
    if values[1] != 1:
        raise ValueError(f"device rejected command: status={values[1]}")
    return True


async def _write_modbus(target: NativeTarget, raw: int) -> None:
    client = AsyncModbusTcpClient(target.host, port=target.port, timeout=target.timeout, retries=0)
    try:
        if not await client.connect():
            raise ConnectionError("Modbus connection failed")

        async def read_ack() -> list[int]:
            reply = await client.read_holding_registers(
                target.ack_address, count=2, slave=target.unit_id
            )
            if reply.isError():
                raise ConnectionError(f"Modbus acknowledgement read failed: {reply}")
            return list(reply.registers)

        sequence = _sequence((await read_ack())[0])
        # Payload first, sequence last: the simulator cannot consume a partial command.
        for address, values in (
            (target.command_address + 1, [target.operation, raw]),
            (target.command_address, [sequence]),
        ):
            reply = await client.write_registers(
                address, [int(value) for value in values], slave=target.unit_id
            )
            if reply.isError():
                raise ConnectionError(f"Modbus write failed: {reply}")
        deadline = time.monotonic() + target.timeout
        while time.monotonic() < deadline:
            if _check_ack(sequence, await read_ack()):
                return
            await asyncio.sleep(0.025)
        raise TimeoutError("device outcome unknown: Modbus acknowledgement timeout")
    finally:
        client.close()


def _write_s7(target: NativeTarget, raw: int) -> None:
    client = S7Client()
    try:
        client.connect(target.host, target.rack, target.slot, tcp_port=target.port)
        previous = struct.unpack(">HH", client.db_read(target.db, target.ack_address, 4))[0]
        sequence = _sequence(previous)
        client.db_write(
            target.db,
            target.command_address + 2,
            bytearray(struct.pack(">HH", target.operation, raw)),
        )
        client.db_write(target.db, target.command_address, bytearray(struct.pack(">H", sequence)))
        deadline = time.monotonic() + target.timeout
        while time.monotonic() < deadline:
            values = list(struct.unpack(">HH", client.db_read(target.db, target.ack_address, 4)))
            if _check_ack(sequence, values):
                return
            time.sleep(0.025)
        raise TimeoutError("device outcome unknown: S7 acknowledgement timeout")
    finally:
        client.disconnect()
        client.destroy()


async def write_native_value(target: NativeTarget, value: object) -> None:
    raw = encode_value(target, value)
    if target.protocol == "modbus":
        await _write_modbus(target, raw)
    else:
        await asyncio.to_thread(_write_s7, target, raw)
