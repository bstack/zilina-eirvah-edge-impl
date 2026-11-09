"""Read-only native fixture servers inside an isolated, loopback-published container."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path

from asyncua import Server, ua
from pymodbus.datastore import ModbusSequentialDataBlock, ModbusServerContext, ModbusSlaveContext
from pymodbus.server import StartAsyncTcpServer
from snap7.server import Server as S7Server
from snap7.type import SrvArea


async def main() -> None:
    directory = Path("/cases")
    opc = Server()
    await opc.init()
    opc.set_endpoint("opc.tcp://0.0.0.0:4840/e2/")
    idx = await opc.register_namespace("urn:eirvah:e2")
    parent = await opc.nodes.objects.add_object(idx, "Signals")
    nodes = {}
    for key, value in (("speed", 0.33), ("temperature", 21.0), ("count", 7)):
        nodes[key] = await parent.add_variable(
            idx, key, value, ua.VariantType.Int64 if key == "count" else ua.VariantType.Double
        )
    block = ModbusSequentialDataBlock(0, [0] * 8)  # type: ignore[no-untyped-call]
    store = ModbusSlaveContext(hr=block)  # type: ignore[no-untyped-call]
    modbus = ModbusServerContext(slaves={1: store}, single=False)  # type: ignore[no-untyped-call]
    db = bytearray(10)
    s7 = S7Server()
    s7.register_area(SrvArea.DB, 1, db)
    s7.start(tcp_port=1102)
    async with opc:
        task = asyncio.create_task(StartAsyncTcpServer(context=modbus, address=("0.0.0.0", 5020)))  # type: ignore[no-untyped-call]
        await asyncio.sleep(0.5)
        if task.done():
            await task
            raise RuntimeError("Modbus server stopped")
        (directory / "ready.json").write_text(json.dumps(dict(ready=True)), encoding="utf-8")
        previous = None
        while True:
            if task.done():
                await task
                raise RuntimeError("Modbus server stopped")
            path = directory / "control.json"
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                await asyncio.sleep(0.05)
                continue
            if data["sequence"] != previous:
                signals = data["signals"]
                for key, node in nodes.items():
                    await node.write_value(
                        signals[key]["opcua"],
                        ua.VariantType.Int64 if key == "count" else ua.VariantType.Double,
                    )
                modbus[1].setValues(
                    3, 0, [signals[k]["modbus"] for k in ("speed", "temperature", "count")]
                )
                db[:] = bytes.fromhex(
                    signals["speed"]["s7"] + signals["temperature"]["s7"] + signals["count"]["s7"]
                )
                previous = data["sequence"]
                (directory / "applied.json").write_text(
                    json.dumps(dict(sequence=previous, applied_at=datetime.now(UTC).isoformat())),
                    encoding="utf-8",
                )
            await asyncio.sleep(0.05)


if __name__ == "__main__":
    asyncio.run(main())
