from unittest.mock import AsyncMock, patch

import pytest
from eirvah_bus.client import BusClient


@pytest.mark.asyncio
async def test_connect_calls_nats_connect_with_servers() -> None:
    fake_nc = AsyncMock()
    with patch("eirvah_bus.client.nats.connect", AsyncMock(return_value=fake_nc)) as conn:
        client = BusClient(servers=["nats://nats:4222"])
        await client.connect()
        conn.assert_awaited_once()
        assert client.nc is fake_nc


@pytest.mark.asyncio
async def test_close_drains_underlying_connection() -> None:
    fake_nc = AsyncMock()
    with patch("eirvah_bus.client.nats.connect", AsyncMock(return_value=fake_nc)):
        client = BusClient(servers=["nats://nats:4222"])
        await client.connect()
        await client.close()
        fake_nc.drain.assert_awaited_once()


@pytest.mark.asyncio
async def test_connect_is_idempotent() -> None:
    fake_nc = AsyncMock()
    with patch("eirvah_bus.client.nats.connect", AsyncMock(return_value=fake_nc)) as conn:
        client = BusClient(servers=["nats://nats:4222"])
        await client.connect()
        await client.connect()
        assert conn.await_count == 1
