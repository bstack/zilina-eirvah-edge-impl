from unittest.mock import AsyncMock

import pytest
from eirvah_bus.request_reply import (
    BUS_HEADER_CORRELATION_ID,
    RequestTimeout,
    request_reply,
)


class _FakeMsg:
    def __init__(self, data: bytes, headers: dict[str, str] | None = None) -> None:
        self.data = data
        self.headers = headers or {}


@pytest.mark.asyncio
async def test_request_reply_propagates_payload_and_header() -> None:
    nc = AsyncMock()
    nc.request = AsyncMock(return_value=_FakeMsg(b'{"ok": true}'))
    correlation_id = "01HZXC8P9G7Q3M6V0K2T8R5W4A"
    payload = b'{"value": 42}'

    reply = await request_reply(
        nc=nc,
        subject="uns.work.convert",
        payload=payload,
        correlation_id=correlation_id,
        timeout_s=1.0,
    )

    nc.request.assert_awaited_once()
    args, kwargs = nc.request.call_args
    assert args[0] == "uns.work.convert"
    assert args[1] == payload
    assert kwargs["timeout"] == 1.0
    assert kwargs["headers"][BUS_HEADER_CORRELATION_ID] == correlation_id
    assert reply.data == b'{"ok": true}'


@pytest.mark.asyncio
async def test_request_reply_translates_asyncio_timeout() -> None:
    nc = AsyncMock()
    nc.request = AsyncMock(side_effect=TimeoutError())
    with pytest.raises(RequestTimeout):
        await request_reply(
            nc=nc,
            subject="uns.work.convert",
            payload=b"{}",
            correlation_id="01HZXC8P9G7Q3M6V0K2T8R5W4A",
            timeout_s=0.1,
        )
