from unittest.mock import AsyncMock

import pytest
from eirvah_bus.consumer import subscribe_queue_group


@pytest.mark.asyncio
async def test_subscribe_queue_group_registers_handler() -> None:
    nc = AsyncMock()

    async def handler(msg) -> None:  # type: ignore[no-untyped-def]
        return None

    await subscribe_queue_group(
        nc=nc,
        subject="uns.work.convert",
        queue="uns.work.convert",
        handler=handler,
    )

    nc.subscribe.assert_awaited_once_with(
        "uns.work.convert",
        queue="uns.work.convert",
        cb=handler,
    )


@pytest.mark.asyncio
async def test_subscribe_queue_group_defaults_queue_to_subject() -> None:
    nc = AsyncMock()

    async def handler(msg) -> None:  # type: ignore[no-untyped-def]
        return None

    await subscribe_queue_group(nc=nc, subject="act.work.validate", handler=handler)

    nc.subscribe.assert_awaited_once_with(
        "act.work.validate",
        queue="act.work.validate",
        cb=handler,
    )
