"""Bounded Modbus fault proxy for a temporary research writer sidecar.

Listens only on pod loopback. Control and evidence use private files accessed
through authenticated kubectl exec; there is no remote control/evidence listener.
No deployment uses this helper unless the explicit fault-case runner installs it.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import struct
import time
from contextlib import suppress
from pathlib import Path
from typing import Any

MODES = {"normal", "disconnect_before_write", "drop_commit", "drop_ack", "hold_ack"}


def frame(pdu: bytes, transaction: int = 1, unit: int = 1) -> bytes:
    return struct.pack(">HHHB", transaction, 0, len(pdu) + 1, unit) + pdu


async def read_frame(reader: asyncio.StreamReader) -> bytes:
    header = await reader.readexactly(6)
    _, protocol, length = struct.unpack(">HHH", header)
    if protocol != 0 or not 2 <= length <= 254:
        raise ValueError("invalid Modbus frame")
    return header + await reader.readexactly(length)


class FaultProxy:
    def __init__(self, host: str, port: int, directory: Path) -> None:
        self.host, self.port, self.directory = host, port, directory
        directory.mkdir(parents=True, exist_ok=True)
        self.sequence = 0

    def plan(self) -> dict[str, Any]:
        path = self.directory / "plan.json"
        plan = (
            json.loads(path.read_text()) if path.exists() else {"id": "startup", "mode": "normal"}
        )
        if plan.get("mode") not in MODES or not isinstance(plan.get("id"), str):
            raise ValueError("invalid fault plan")
        return dict(plan)

    def record(self, plan: dict[str, Any], event: str, **values: Any) -> None:
        if self.sequence >= 4096:
            raise RuntimeError("fault evidence capacity reached")
        self.sequence += 1
        row = dict(
            case_id=plan["id"],
            mode=plan["mode"],
            command_id=plan.get("command_id"),
            event=event,
            record_seq=self.sequence,
            wall=time.time(),
            monotonic_ns=time.monotonic_ns(),
            process=os.getpid(),
            **values,
        )
        with (self.directory / "events.jsonl").open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(row) + "\n")
            stream.flush()
            os.fsync(stream.fileno())

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        upstream = None
        try:
            plan = self.plan()
            self.record(plan, "connected")
            if plan["mode"] == "disconnect_before_write":
                self.record(plan, "disconnected_before_write")
                return
            remote, upstream = await asyncio.open_connection(self.host, self.port)
            committed = None
            while True:
                request = await asyncio.wait_for(read_frame(reader), 45)
                pdu = request[7:]
                function = pdu[0]
                address = int.from_bytes(pdu[1:3], "big") if len(pdu) >= 3 else -1
                count = int.from_bytes(pdu[3:5], "big") if len(pdu) >= 5 else -1
                commit = function == 16 and address == 28 and count == 1 and len(pdu) == 8
                if function == 16 and not ((address == 29 and count == 2) or commit):
                    raise ValueError("proxy permits only the declared rejection mailbox")
                if function != 16 and not (function == 3 and address == 54 and count == 2):
                    raise ValueError("proxy permits only the declared acknowledgement")
                if commit:
                    committed = int.from_bytes(pdu[6:8], "big")
                    if plan["mode"] == "drop_commit":
                        self.record(plan, "commit_dropped", sequence=committed)
                        return
                upstream.write(request)
                await upstream.drain()
                response = await asyncio.wait_for(read_frame(remote), 35)
                if commit:
                    self.record(plan, "commit_forwarded", sequence=committed)
                if (
                    function == 3
                    and committed is not None
                    and len(response) == 13
                    and response[7:9] == bytes([3, 4])
                ):
                    sequence, status = struct.unpack(">HH", response[9:13])
                    if sequence == committed and status == 1:
                        if plan["mode"] == "drop_ack":
                            self.record(plan, "ack_dropped", sequence=sequence, status=status)
                            return
                        if plan["mode"] == "hold_ack":
                            self.record(plan, "ack_held", sequence=sequence, status=status)
                            deadline = time.monotonic() + 35
                            while self.plan()["mode"] == "hold_ack" and time.monotonic() < deadline:
                                await asyncio.sleep(0.05)
                            if self.plan()["mode"] == "hold_ack":
                                return
                        self.record(plan, "ack_forwarded", sequence=sequence, status=status)
                writer.write(response)
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError, TimeoutError):
            pass
        finally:
            writer.close()
            if upstream is not None:
                upstream.close()
                with suppress(ConnectionError):
                    await upstream.wait_closed()
            with suppress(ConnectionError):
                await writer.wait_closed()


async def serve(host: str, port: int, upstream: int, directory: Path) -> None:
    proxy = FaultProxy(host, upstream, directory)
    server = await asyncio.start_server(proxy.handle, "127.0.0.1", port)
    (directory / "ready").write_text("loopback-only")
    async with server:
        await server.serve_forever()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, default=15020)
    parser.add_argument("--upstream-port", type=int, default=5020)
    parser.add_argument("--directory", type=Path, default=Path("/tmp/e0-native-faults"))
    args = parser.parse_args()
    asyncio.run(serve(args.host, args.port, args.upstream_port, args.directory))


if __name__ == "__main__":
    main()
