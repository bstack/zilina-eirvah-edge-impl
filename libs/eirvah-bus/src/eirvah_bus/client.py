"""Lifecycle wrapper around a NATS client connection."""

from __future__ import annotations

from collections.abc import Sequence

import nats
from nats.aio.client import Client as NATSClient


class BusClient:
    """Owns a single NATS connection for the life of a service process."""

    def __init__(self, servers: Sequence[str], name: str | None = None) -> None:
        self._servers: list[str] = list(servers)
        self._name = name
        self._nc: NATSClient | None = None

    @property
    def nc(self) -> NATSClient:
        if self._nc is None:
            raise RuntimeError("BusClient.connect() must be awaited before use")
        return self._nc

    @property
    def connected(self) -> bool:
        return self._nc is not None and self._nc.is_connected

    async def connect(self) -> None:
        """Establish the NATS connection; idempotent."""
        if self._nc is not None:
            return
        self._nc = await nats.connect(servers=self._servers, name=self._name)

    async def close(self) -> None:
        """Drain in-flight messages and close the connection."""
        if self._nc is None:
            return
        await self._nc.drain()
        self._nc = None
