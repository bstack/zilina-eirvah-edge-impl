"""Real TCP frame faults are deterministic and stay on the selected mailbox."""

import asyncio
import json
import struct
from contextlib import suppress

import pytest
from native_fault_proxy import FaultProxy, frame


@pytest.mark.parametrize(
    "mode,effect,ack",
    [
        ("normal", 1, True),
        ("disconnect_before_write", 0, False),
        ("drop_commit", 0, False),
        ("drop_ack", 1, False),
    ],
)
async def test_frame_faults_distinguish_zero_and_one_native_effect(tmp_path, mode, effect, ack):
    from native_fault_proxy import read_frame

    effects = []
    sequence = [0]

    async def native(reader, writer):
        try:
            while True:
                packet = await read_frame(reader)
                pdu = packet[7:]
                function = pdu[0]
                address = int.from_bytes(pdu[1:3], "big")
                if function == 16:
                    if address == 28:
                        sequence[0] = int.from_bytes(pdu[6:8], "big")
                        effects.append(sequence[0])
                    reply = bytes([16]) + pdu[1:5]
                else:
                    reply = bytes([3, 4]) + struct.pack(">HH", sequence[0], 1)
                writer.write(frame(reply, int.from_bytes(packet[:2], "big")))
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.close()
            with suppress(ConnectionError):
                await writer.wait_closed()

    server = await asyncio.start_server(native, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    (tmp_path / "plan.json").write_text(
        json.dumps({"id": "case", "mode": mode, "command_id": "command"})
    )
    proxy = FaultProxy("127.0.0.1", port, tmp_path)
    front = await asyncio.start_server(proxy.handle, "127.0.0.1", 0)
    reader, writer = await asyncio.open_connection("127.0.0.1", front.sockets[0].getsockname()[1])
    succeeded = False
    try:
        for payload in [
            bytes([3]) + struct.pack(">HH", 54, 2),
            bytes([16]) + struct.pack(">HHBHH", 29, 2, 4, 1, 10),
            bytes([16]) + struct.pack(">HHBH", 28, 1, 2, 7),
            bytes([3]) + struct.pack(">HH", 54, 2),
        ]:
            writer.write(frame(payload))
            await writer.drain()
            await asyncio.wait_for(read_frame(reader), 2)
        succeeded = True
    except (asyncio.IncompleteReadError, ConnectionError):
        pass
    finally:
        writer.close()
        with suppress(ConnectionError):
            await writer.wait_closed()
        front.close()
        await front.wait_closed()
        server.close()
        await server.wait_closed()
    await asyncio.sleep(0)

    assert len(effects) == effect
    assert succeeded is ack
    records = [json.loads(line) for line in (tmp_path / "events.jsonl").read_text().splitlines()]
    assert all(r["case_id"] == "case" for r in records)
    if mode == "drop_ack":
        assert any(
            r["event"] == "ack_dropped" and r["sequence"] == 7 and r["status"] == 1 for r in records
        )


async def test_invalid_mbap_frame_is_refused():
    from native_fault_proxy import read_frame

    reader = asyncio.StreamReader()
    reader.feed_data(b"\x00\x01\x00\x00\xff\xff")

    with pytest.raises(ValueError, match="frame"):
        await read_frame(reader)


async def test_held_successful_ack_can_be_released_without_another_effect(tmp_path):
    from native_fault_proxy import read_frame

    sequence = [0]
    effects = []

    async def native(reader, writer):
        try:
            while True:
                request = await read_frame(reader)
                pdu = request[7:]
                if pdu[0] == 16:
                    if int.from_bytes(pdu[1:3], "big") == 28:
                        sequence[0] = int.from_bytes(pdu[6:8], "big")
                        effects.append(sequence[0])
                    reply = bytes([16]) + pdu[1:5]
                else:
                    reply = bytes([3, 4]) + struct.pack(">HH", sequence[0], 1)
                writer.write(frame(reply))
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.close()
            with suppress(ConnectionError):
                await writer.wait_closed()

    server = await asyncio.start_server(native, "127.0.0.1", 0)
    plan = {"id": "held", "mode": "hold_ack", "command_id": "command"}
    (tmp_path / "plan.json").write_text(json.dumps(plan))
    proxy = FaultProxy("127.0.0.1", server.sockets[0].getsockname()[1], tmp_path)
    front = await asyncio.start_server(proxy.handle, "127.0.0.1", 0)
    reader, writer = await asyncio.open_connection("127.0.0.1", front.sockets[0].getsockname()[1])
    writer.write(frame(bytes([16]) + struct.pack(">HHBHH", 29, 2, 4, 1, 10)))
    await writer.drain()
    await read_frame(reader)
    writer.write(frame(bytes([16]) + struct.pack(">HHBH", 28, 1, 2, 7)))
    await writer.drain()
    await read_frame(reader)
    writer.write(frame(bytes([3]) + struct.pack(">HH", 54, 2)))
    await writer.drain()
    response = asyncio.create_task(read_frame(reader))
    for _ in range(50):
        if "ack_held" in (tmp_path / "events.jsonl").read_text():
            break
        await asyncio.sleep(0.01)

    assert effects == [7]
    assert not response.done()
    (tmp_path / "plan.json").write_text(json.dumps({**plan, "mode": "normal"}))
    packet = await asyncio.wait_for(response, 2)

    assert struct.unpack(">HH", packet[9:13]) == (7, 1)
    assert effects == [7]
    writer.close()
    with suppress(ConnectionError):
        await writer.wait_closed()
    front.close()
    await front.wait_closed()
    server.close()
    await server.wait_closed()
