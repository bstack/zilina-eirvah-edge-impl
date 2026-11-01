"""Real loopback protocol checks; no Kubernetes resources or plant connections."""

from __future__ import annotations

import asyncio
import socket
from contextlib import suppress
from unittest.mock import patch

from actuation_signal_publisher.native import NativeTarget, write_native_value


def unused_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


async def test_modbus_write_is_applied_and_acknowledged_over_tcp() -> None:
    from modbus_simulator.config import SimulatorSettings
    from modbus_simulator.server import SimulatorRuntime
    from pymodbus.server import ServerAsyncStop

    port = unused_port()
    with patch("modbus_simulator.server.SimulatorMetrics"):
        runtime = SimulatorRuntime(SimulatorSettings(host="127.0.0.1", port=port, tick_rate_ms=50))
    task = asyncio.create_task(runtime.run())
    try:
        await asyncio.sleep(0.2)
        target = NativeTarget(
            protocol="modbus",
            host="127.0.0.1",
            port=port,
            command_address=20,
            ack_address=50,
            operation=2,
            value_type="double",
            scale=100,
            minimum=0,
            maximum=0.8,
        )
        await write_native_value(target, 0.65)
        assert runtime._block.conveyor.setpoint == 65
        await write_native_value(
            target.model_copy(
                update={"operation": 1, "value_type": "bool", "scale": 1, "maximum": 1}
            ),
            False,
        )
        assert not runtime._block.conveyor.running
        valve = target.model_copy(
            update={
                "command_address": 24,
                "ack_address": 52,
                "operation": 1,
                "value_type": "bool",
                "scale": 1,
                "maximum": 1,
            }
        )
        await write_native_value(valve, False)
        assert not runtime._block.valve.opened and runtime._block.conveyor.setpoint == 65
        runtime._block.conveyor.apply(4, 1)  # deterministic fault fixture
        reset = target.model_copy(
            update={"operation": 3, "value_type": "bool", "scale": 1, "minimum": 1, "maximum": 1}
        )
        await write_native_value(reset, True)
        assert not runtime._block.conveyor.faulted and not runtime._block.conveyor.running
        runtime._block.reject.observe(42)
        reject = target.model_copy(
            update={
                "command_address": 28,
                "ack_address": 54,
                "operation": 1,
                "value_type": "int64",
                "scale": 1,
                "minimum": 1,
                "maximum": 65535,
            }
        )
        await write_native_value(reject, 42)
        assert runtime._block.reject.count == 1
        import pytest

        with pytest.raises(ValueError, match="status=4"):
            await write_native_value(reject, 42)
        assert runtime._block.reject.count == 1
    finally:
        await ServerAsyncStop()
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


async def test_s7_write_is_applied_and_acknowledged_over_tcp() -> None:
    from s7_simulator.config import SimulatorSettings
    from s7_simulator.server import SimulatorRuntime
    from snap7.server import Server
    from snap7.type import SrvArea

    port = unused_port()
    with patch("s7_simulator.server.SimulatorMetrics"):
        runtime = SimulatorRuntime(SimulatorSettings())
    server = Server()
    server.register_area(SrvArea.DB, 3, runtime._db3)
    runtime._sync_buffers()
    server.start(tcp_port=port)

    async def cycle() -> None:
        while True:
            runtime._process_controls()
            runtime._sync_buffers()
            await asyncio.sleep(0.02)

    task = asyncio.create_task(cycle())
    try:
        target = NativeTarget(
            protocol="s7",
            host="127.0.0.1",
            port=port,
            command_address=0,
            ack_address=6,
            operation=2,
            value_type="int64",
            minimum=0,
            maximum=1800,
        )
        await write_native_value(target, 1100)
        assert runtime._capper.motor.speed == 1100
        await write_native_value(
            target.model_copy(update={"operation": 1, "value_type": "bool", "maximum": 1}), False
        )
        assert runtime._capper.motor.speed == 0
        assert runtime._palletizer.layer_count == 0
        runtime._capper.motor.apply(4, 1)
        reset = target.model_copy(
            update={"operation": 3, "value_type": "bool", "minimum": 1, "maximum": 1}
        )
        await write_native_value(reset, True)
        assert not runtime._capper.motor.faulted and not runtime._capper.motor.running
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
        server.stop()
        server.destroy()
