"""Entry point for the OPC UA data subscriber pod."""

from __future__ import annotations

import asyncio

from opcua_data_subscriber.config import SubscriberSettings
from opcua_data_subscriber.service import run


def main() -> None:
    settings = SubscriberSettings()
    asyncio.run(run(settings))


if __name__ == "__main__":  # pragma: no cover
    main()
